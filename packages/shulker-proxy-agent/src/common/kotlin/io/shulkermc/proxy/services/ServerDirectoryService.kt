package io.shulkermc.proxy.services

import io.shulkermc.cluster.api.adapters.cache.CacheAdapter
import io.shulkermc.cluster.api.adapters.cache.RedisCacheAdapter
import io.shulkermc.cluster.api.adapters.kubernetes.WatchAction
import io.shulkermc.cluster.api.adapters.kubernetes.models.AgonesV1GameServer
import io.shulkermc.proxy.Configuration
import io.shulkermc.proxy.ShulkerProxyAgentCommon
import io.shulkermc.proxy.adapters.filesystem.FileSystemAdapter
import io.shulkermc.proxy.utils.addressFromHostString
import java.net.InetSocketAddress
import java.util.Optional
import java.util.concurrent.ConcurrentHashMap

class ServerDirectoryService(
    private val agent: ShulkerProxyAgentCommon,
) {
    companion object {
        private const val AGONES_FLEET_LABEL = "agones.dev/fleet"
        private const val SERVER_TAGS_ANNOTATION = "minecraftserver.shulkermc.io/tags"
    }

    private val serversByTag = HashMap<String, MutableSet<String>>()
    private val tagsByServer = HashMap<String, Set<String>>()

    /** Names of servers coming from K8s GameServers or the CR ConfigMap. They always win over Redis. */
    private val authoritativeServers = ConcurrentHashMap.newKeySet<String>()

    private var externalServers: Optional<Map<String, FileSystemAdapter.ExternalServer>> = Optional.empty()

    init {
        this.agent.cluster.kubernetesGateway.watchMinecraftServerEvents { action, minecraftServer ->
            this.agent.logger.fine("Detected modification on MinecraftServer '${minecraftServer.metadata.name}'")
            if (action == WatchAction.ADDED || action == WatchAction.MODIFIED) {
                this.registerManagedServer(minecraftServer)
            } else if (action == WatchAction.DELETED) {
                this.unregisterServer(minecraftServer.metadata.name)
            }
        }

        val existingMinecraftServers = this.agent.cluster.kubernetesGateway.listMinecraftServers()
        existingMinecraftServers.items
            .filterNotNull()
            .forEach(this::registerManagedServer)

        this.agent.fileSystem.watchExternalServersUpdates(this::onExternalServersUpdate)

        this.agent.cluster.pubSub.subscribe(RedisCacheAdapter.EXTERNAL_SERVER_REGISTER_CHANNEL) { serverName ->
            this.onDynamicExternalServerRegister(serverName)
        }
        this.agent.cluster.pubSub.subscribe(RedisCacheAdapter.EXTERNAL_SERVER_UNREGISTER_CHANNEL) { serverName ->
            this.onDynamicExternalServerUnregister(serverName)
        }

        this.agent.cluster.cache.listExternalDynamicServers().forEach { server ->
            this.onDynamicExternalServerRegister(server.name)
        }
    }

    fun getServersByTag(tag: String): Set<String> = this.serversByTag.getOrDefault(tag, setOf())

    private fun registerManagedServer(minecraftServer: AgonesV1GameServer) {
        if (minecraftServer.status == null || !minecraftServer.status.isReady()) {
            return
        }

        val tags =
            minecraftServer.metadata.annotations
                ?.get(SERVER_TAGS_ANNOTATION)
                ?.split(",")
                ?.map { it.trim() }
                ?.filter { it.isNotEmpty() }
                ?.toSet()
                .orEmpty()
        val fleetName = minecraftServer.metadata.labels?.get(AGONES_FLEET_LABEL)
        val address =
            addressFromHostString(
                "${minecraftServer.metadata.name}.${Configuration.CLUSTER_NAME}-cluster.${minecraftServer.metadata.namespace}",
            )

        this.authoritativeServers.add(minecraftServer.metadata.name)
        // Capacity is published by the server agent from SHULKER_SERVER_MAX_PLAYERS (fleet/server config).
        this.registerServer(
            name = minecraftServer.metadata.name,
            address = address,
            tags = tags,
            source = CacheAdapter.SOURCE_MANAGED,
            fleetName = fleetName,
        )
    }

    private fun registerServer(
        name: String,
        address: InetSocketAddress,
        tags: Set<String>,
        source: String,
        fleetName: String? = null,
        maxPlayers: Int = 0,
    ) {
        val alreadyOnProxy = this.agent.proxyInterface.hasServer(name)
        if (!alreadyOnProxy) {
            this.agent.proxyInterface.registerServer(name, address)
        }

        val previousTags = this.tagsByServer[name].orEmpty()
        previousTags.forEach { tag -> this.serversByTag[tag]?.remove(name) }
        for (tag in tags) {
            this.serversByTag.getOrPut(tag) { mutableSetOf() }.add(name)
            this.agent.logger.fine("Tagged '$tag' on server '$name'")
        }
        this.tagsByServer[name] = tags

        val addressString =
            if (address.port > 0) {
                "${address.hostString}:${address.port}"
            } else {
                address.hostString
            }

        this.agent.cluster.cache.registerServer(
            name = name,
            fleetName = fleetName,
            tags = tags.toList(),
            maxPlayers = maxPlayers,
            acceptingPlayers = null,
            address = addressString,
            source = source,
        )

        if (!alreadyOnProxy) {
            this.agent.logger.info("Added server '$name' to directory (source=$source)")
        }
    }

    private fun unregisterServer(name: String) {
        this.authoritativeServers.remove(name)

        val removedFromProxy = this.agent.proxyInterface.unregisterServer(name)
        val tags = this.tagsByServer.remove(name)
        tags?.forEach { tag -> this.serversByTag[tag]?.remove(name) }

        this.agent.cluster.cache.unregisterServer(name)

        if (removedFromProxy) {
            this.agent.logger.info("Removed server '$name' from directory")
        }
    }

    private fun onExternalServersUpdate(servers: Map<String, FileSystemAdapter.ExternalServer>) {
        this.agent.logger.info("External servers file was updated, updating directory")

        this.externalServers.ifPresent { existingServers ->
            existingServers.keys.forEach { name ->
                this.authoritativeServers.remove(name)
                this.unregisterServer(name)
            }
        }

        this.externalServers = Optional.of(servers)

        servers.values.forEach { server ->
            this.authoritativeServers.add(server.name)
            this.registerServer(
                name = server.name,
                address = server.address,
                tags = server.tags,
                source = CacheAdapter.SOURCE_EXTERNAL_CONFIG,
            )
        }
    }

    private fun onDynamicExternalServerRegister(serverName: String) {
        if (this.authoritativeServers.contains(serverName)) {
            this.agent.logger.fine(
                "Ignoring dynamic external server '$serverName': authoritative source already owns this name",
            )
            return
        }

        val maybeServer = this.agent.cluster.cache.getServer(serverName)
        if (maybeServer.isEmpty) {
            this.agent.logger.warning("Received dynamic external register for unknown server '$serverName'")
            return
        }

        val server = maybeServer.get()
        val address = server.address
        if (address.isNullOrBlank()) {
            this.agent.logger.warning("Dynamic external server '$serverName' has no address, skipping")
            return
        }

        this.registerServer(
            name = server.name,
            address = addressFromHostString(address),
            tags = server.tags.toSet(),
            source = CacheAdapter.SOURCE_EXTERNAL_DYNAMIC,
            fleetName = server.fleetName,
            maxPlayers = server.maxPlayers,
        )
    }

    private fun onDynamicExternalServerUnregister(serverName: String) {
        if (this.authoritativeServers.contains(serverName)) {
            return
        }

        this.unregisterServer(serverName)
    }
}
