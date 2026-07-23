package io.shulkermc.cluster.api.adapters.cache

import io.shulkermc.cluster.api.data.PlayerPosition
import io.shulkermc.cluster.api.data.RegisteredProxy
import io.shulkermc.cluster.api.data.RegisteredServer
import java.util.Optional
import java.util.UUID

interface CacheAdapter {
    // Proxies

    fun registerProxy(
        proxyName: String,
        proxyCapacity: Int,
        fleetName: String? = null,
        tags: List<String> = emptyList(),
        acceptingPlayers: Boolean = true,
    )

    fun unregisterProxy(proxyName: String)

    fun updateProxyLastSeen(proxyName: String)

    fun updateProxyAcceptingPlayers(
        proxyName: String,
        acceptingPlayers: Boolean,
    )

    fun listRegisteredProxies(): List<RegisteredProxy>

    fun getProxy(proxyName: String): Optional<RegisteredProxy>

    fun listProxiesByTag(tag: String): List<RegisteredProxy>

    fun tryLockLostProxiesPurgeTask(ownerProxyName: String): Optional<Lock>

    // Servers

    /**
     * Upserts server metadata in the cache. Empty servers are kept so that
     * plugins can discover them without waiting for a first player.
     *
     * @param source managed | external-config | external-dynamic
     */
    fun registerServer(
        name: String,
        fleetName: String?,
        tags: List<String>,
        maxPlayers: Int = 0,
        acceptingPlayers: Boolean? = null,
        address: String? = null,
        source: String = SOURCE_MANAGED,
    )

    fun unregisterServer(serverName: String)

    fun updateServerAcceptingPlayers(
        name: String,
        acceptingPlayers: Boolean,
    )

    fun updateServerMaxPlayers(
        name: String,
        maxPlayers: Int,
    )

    fun listAllServers(): List<RegisteredServer>

    fun listServersByTag(tag: String): List<RegisteredServer>

    fun getServer(name: String): Optional<RegisteredServer>

    fun registerExternalServer(
        name: String,
        address: String,
        tags: List<String>,
        maxPlayers: Int,
    ): Boolean

    fun unregisterExternalServer(name: String): Boolean

    fun listExternalDynamicServers(): List<RegisteredServer>

    // Players

    fun listPlayersInServer(serverName: String): List<UUID>

    fun setPlayerPosition(
        playerId: UUID,
        proxyName: String,
        serverName: String,
    )

    fun unsetPlayerPosition(playerId: UUID)

    fun getPlayerPosition(playerId: UUID): Optional<PlayerPosition>

    fun isPlayerConnected(playerId: UUID): Boolean

    fun updateCachedPlayerName(
        playerId: UUID,
        playerName: String,
    )

    fun getPlayerNameFromId(playerId: UUID): Optional<String>

    fun getPlayerIdFromName(playerName: String): Optional<UUID>

    fun getPlayerNamesFromIds(playerIds: List<UUID>): Map<UUID, String>

    fun countOnlinePlayers(): Int

    fun countPlayerCapacity(): Int

    interface Lock : AutoCloseable

    companion object {
        const val SOURCE_MANAGED = "managed"
        const val SOURCE_EXTERNAL_CONFIG = "external-config"
        const val SOURCE_EXTERNAL_DYNAMIC = "external-dynamic"
    }
}
