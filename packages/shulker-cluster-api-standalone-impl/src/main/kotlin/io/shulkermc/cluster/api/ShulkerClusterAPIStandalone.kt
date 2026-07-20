package io.shulkermc.cluster.api

import io.shulkermc.cluster.api.adapters.cache.CacheAdapter
import io.shulkermc.cluster.api.adapters.cache.RedisCacheAdapter
import io.shulkermc.cluster.api.adapters.mojang.HttpMojangGatewayAdapter
import io.shulkermc.cluster.api.adapters.mojang.MojangGatewayAdapter
import io.shulkermc.cluster.api.adapters.pubsub.RedisPubSubAdapter
import io.shulkermc.cluster.api.data.PlayerPosition
import io.shulkermc.cluster.api.data.RegisteredProxy
import io.shulkermc.cluster.api.data.RegisteredServer
import io.shulkermc.cluster.api.messaging.MessagingBus
import io.shulkermc.cluster.api.standalone.StandaloneConfiguration
import io.shulkermc.sdk.ShulkerSDK
import net.kyori.adventure.text.Component
import redis.clients.jedis.JedisPool
import java.io.Closeable
import java.util.Optional
import java.util.UUID
import java.util.logging.Level
import java.util.logging.Logger

/**
 * A [ShulkerClusterAPI] implementation for processes running *outside* the
 * Kubernetes cluster, as long as they have network access to the same
 * Redis instance the cluster uses for its coordination layer (proxies,
 * servers, tags, player positions, pub/sub events).
 * <br>
 * This intentionally reuses [RedisCacheAdapter]/[RedisPubSubAdapter] from
 * this module rather than reimplementing them - the Redis layout is
 * exactly the one described there, so an external tool built on top of
 * this class stays in sync "for free" with in-cluster agents (proxy-agent,
 * server-agent), and any future fix to that layout benefits both.
 * <br>
 * What is NOT available here compared to [ShulkerClusterAPIImpl]:
 * - [operator] - the Shulker Operator gRPC endpoint is only reachable
 *   in-cluster; calling it throws [UnsupportedOperationException].
 * - This instance never registers itself as a proxy or a server. It is a
 *   read/actor client of the registry, not a participant in it - there is
 *   no Agones sidecar and no Kubernetes API to identify "who am I" outside
 *   the cluster, so none of that self-registration logic applies here.
 */
class ShulkerClusterAPIStandalone(
    val logger: Logger,
    configuration: StandaloneConfiguration = StandaloneConfiguration.fromEnvironment(),
) : ShulkerClusterAPI(), Closeable {
    val jedisPool: JedisPool
    val cache: CacheAdapter
    val pubSub: RedisPubSubAdapter
    val mojangGateway: MojangGatewayAdapter

    init {
        this.logger.info("Connecting to Redis at ${configuration.redis.host}:${configuration.redis.port}")

        this.jedisPool = configuration.redis.createJedisPool()
        this.jedisPool.resource.use { jedis -> jedis.ping() }

        this.cache = RedisCacheAdapter(this.jedisPool)
        this.pubSub = RedisPubSubAdapter(configuration.clientName, this.jedisPool)
        this.mojangGateway = HttpMojangGatewayAdapter()

        this.logger.info("Connected as '${configuration.clientName}'")
    }

    override fun close() {
        try {
            this.pubSub.close()
            this.jedisPool.destroy()
        } catch (
            @Suppress("TooGenericExceptionCaught") e: Exception,
        ) {
            this.logger.log(Level.SEVERE, "Failed to properly destroy adapters", e)
        }
    }

    override fun operator(): ShulkerSDK =
        throw UnsupportedOperationException(
            "The Shulker Operator gRPC endpoint is only reachable from inside the cluster",
        )

    override fun messaging(): MessagingBus = this.pubSub

    override fun teleportPlayerOnServer(
        playerId: UUID,
        serverName: String,
    ) = this.pubSub.teleportPlayerOnServer(playerId, serverName)

    override fun disconnectPlayerFromCluster(
        playerId: UUID,
        message: Component,
    ) = this.pubSub.disconnectPlayerFromCluster(playerId, message)

    override fun reconnectPlayerToCluster(playerId: UUID) = this.pubSub.reconnectPlayerToCluster(playerId)

    override fun getPlayerPosition(playerId: UUID): Optional<PlayerPosition> = this.cache.getPlayerPosition(playerId)

    override fun isPlayerConnected(playerId: UUID): Boolean = this.cache.isPlayerConnected(playerId)

    override fun countOnlinePlayers(): Int = this.cache.countOnlinePlayers()

    override fun getPlayerIdFromName(playerName: String): Optional<UUID> {
        val cachedValue = this.cache.getPlayerIdFromName(playerName)
        if (cachedValue.isPresent) return cachedValue

        val mojangProfile = this.mojangGateway.getProfileFromName(playerName)
        if (mojangProfile.isPresent) {
            val playerId = mojangProfile.get().playerId
            this.cache.updateCachedPlayerName(playerId, playerName)
            return Optional.of(playerId)
        }

        return Optional.empty()
    }

    override fun getPlayerNameFromId(playerId: UUID): Optional<String> {
        val cachedValue = this.cache.getPlayerNameFromId(playerId)
        if (cachedValue.isPresent) return cachedValue

        val mojangProfile = this.mojangGateway.getProfileFromId(playerId)
        if (mojangProfile.isPresent) {
            val playerName = mojangProfile.get().playerName
            this.cache.updateCachedPlayerName(playerId, playerName)
            return Optional.of(playerName)
        }

        return Optional.empty()
    }

    override fun listProxies(): List<RegisteredProxy> = this.cache.listRegisteredProxies()

    override fun getProxyById(proxyName: String): Optional<RegisteredProxy> = this.cache.getProxy(proxyName)

    override fun getProxiesByTag(tag: String): List<RegisteredProxy> = this.cache.listProxiesByTag(tag)

    override fun listServers(): List<RegisteredServer> = this.cache.listAllServers()

    override fun getServerById(serverName: String): Optional<RegisteredServer> = this.cache.getServer(serverName)

    override fun getServersByTag(tag: String): List<RegisteredServer> = this.cache.listServersByTag(tag)

    override fun registerExternalServer(
        name: String,
        address: String,
        tags: List<String>,
        maxPlayers: Int,
    ) {
        this.cache.registerExternalServer(name, address, tags, maxPlayers)
    }

    override fun unregisterExternalServer(name: String) {
        this.cache.unregisterExternalServer(name)
    }

    override fun getPlayersInServer(serverName: String): List<UUID> = this.cache.listPlayersInServer(serverName)

    override fun updateServerAcceptingPlayers(
        serverName: String,
        acceptingPlayers: Boolean,
    ) {
        this.cache.updateServerAcceptingPlayers(serverName, acceptingPlayers)
    }
}
