package io.shulkermc.cluster.api.adapters.cache

import io.shulkermc.cluster.api.data.PlayerPosition
import io.shulkermc.cluster.api.data.RegisteredProxy
import io.shulkermc.cluster.api.data.RegisteredServer
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.params.SetParams
import java.time.Instant
import java.util.Optional
import java.util.UUID

/**
 * Unified Redis layout. Every entity (proxy, server, player) follows the
 * same shape: a SET of ids, one HASH per id, and where relevant SETs for
 * the reverse side of a relation - never more than the two directions of
 * a relation (e.g. player->proxy lives in the player HASH, proxy->players
 * lives in the proxy's reverse SET; nothing else references a player).
 *
 * ```
 * shulker:proxies                         SET  proxy names
 * shulker:proxies:info:{name}             HASH capacity, lastSeen, acceptingPlayers, fleetName, tags
 * shulker:proxies:info:{name}:players     SET  player ids currently on this proxy
 * shulker:proxies:by-tag:{tag}            SET  proxy names
 *
 * shulker:servers                         SET  server names
 * shulker:servers:info:{name}             HASH capacity, fleetName, tags, address, source, acceptingPlayers, lastUpdated
 * shulker:servers:info:{name}:players     SET  player ids currently on this server
 * shulker:servers:by-tag:{tag}            SET  server names
 * shulker:servers:external-dynamic        SET  dynamically registered external servers
 *
 * shulker:players                         SET  ids of all online players
 * shulker:players:{id}                    HASH proxy, server, lastSeen
 *
 * shulker:uuid-cache:id-to-name:{id}      STRING (TTL)
 * shulker:uuid-cache:name-to-id:{name}    STRING (TTL)
 * shulker:locks:...                       STRING (TTL)
 *
 * shulker:events:server-updated                      PUBLISH server name
 * shulker:events:external-server-register            PUBLISH server name
 * shulker:events:external-server-unregister           PUBLISH server name
 * shulker:events:player-position-changed              PUBLISH player id
 * ```
 *
 * A player's own HASH is the single source of truth for "where is this
 * player" (proxy + server + lastSeen). The proxy/server `:players` SETs
 * only exist so that "who is on X" doesn't require scanning every online
 * player - they are a pure reverse index, kept in sync by
 * setPlayerPosition/unsetPlayerPosition and never written to directly.
 */
class RedisCacheAdapter(private val jedisPool: JedisPool) : CacheAdapter {
    companion object {
        private const val PROXY_LOST_PURGE_LOCK_SECONDS = 15L
        private const val PLAYER_ID_CACHE_TTL_SECONDS = 60L * 60 * 24 * 14

        private const val KEY_PREFIX = "shulker"

        // Proxies
        private const val PROXIES_SET_KEY = "$KEY_PREFIX:proxies"
        private val PROXIES_INFO_HASH_KEY = { name: String -> "$KEY_PREFIX:proxies:info:$name" }
        private val PROXIES_PLAYERS_SET_KEY = { name: String -> "$KEY_PREFIX:proxies:info:$name:players" }
        private val PROXIES_BY_TAG_SET_KEY = { tag: String -> "$KEY_PREFIX:proxies:by-tag:$tag" }

        // Servers
        private const val SERVERS_SET_KEY = "$KEY_PREFIX:servers"
        private const val SERVERS_EXTERNAL_DYNAMIC_SET_KEY = "$KEY_PREFIX:servers:external-dynamic"
        private val SERVERS_INFO_HASH_KEY = { name: String -> "$KEY_PREFIX:servers:info:$name" }
        private val SERVERS_PLAYERS_SET_KEY = { name: String -> "$KEY_PREFIX:servers:info:$name:players" }
        private val SERVERS_BY_TAG_SET_KEY = { tag: String -> "$KEY_PREFIX:servers:by-tag:$tag" }

        // Players - one HASH per player is the sole source of truth for presence + location
        private const val PLAYERS_SET_KEY = "$KEY_PREFIX:players"
        private val PLAYERS_HASH_KEY = { id: String -> "$KEY_PREFIX:players:$id" }

        // UUID name cache (Mojang lookups, unrelated to online presence)
        private const val UUID_CACHE_KEY_PREFIX = "$KEY_PREFIX:uuid-cache"
        private val UUID_CACHE_NAME_TO_ID_KEY = { name: String -> "$UUID_CACHE_KEY_PREFIX:name-to-id:$name" }
        private val UUID_CACHE_ID_TO_NAME_KEY = { id: String -> "$UUID_CACHE_KEY_PREFIX:id-to-name:$id" }

        // Locks
        private const val LOCKS_LOST_PROXIES_PURGE_KEY = "$KEY_PREFIX:locks:lost-proxies-purge"

        // Pub/Sub
        const val SERVER_UPDATED_CHANNEL = "$KEY_PREFIX:events:server-updated"
        const val EXTERNAL_SERVER_REGISTER_CHANNEL = "$KEY_PREFIX:events:external-server-register"
        const val EXTERNAL_SERVER_UNREGISTER_CHANNEL = "$KEY_PREFIX:events:external-server-unregister"
        const val PLAYER_POSITION_CHANGED_CHANNEL = "$KEY_PREFIX:events:player-position-changed"

        // Info hash fields (shared naming for proxies & servers where applicable)
        private const val FIELD_CAPACITY = "capacity"
        private const val FIELD_LAST_SEEN = "lastSeen"
        private const val FIELD_ACCEPTING_PLAYERS = "acceptingPlayers"
        private const val FIELD_FLEET_NAME = "fleetName"
        private const val FIELD_TAGS = "tags"
        private const val FIELD_ADDRESS = "address"
        private const val FIELD_SOURCE = "source"
        private const val FIELD_LAST_UPDATED = "lastUpdated"

        // Player hash fields
        private const val FIELD_PROXY = "proxy"
        private const val FIELD_SERVER = "server"
    }

    // ==================== Proxies ====================

    override fun registerProxy(
        proxyName: String,
        proxyCapacity: Int,
        fleetName: String?,
        tags: List<String>,
        acceptingPlayers: Boolean,
    ) {
        this.jedisPool.resource.use { jedis ->
            val infoKey = PROXIES_INFO_HASH_KEY(proxyName)
            val previousTags = parseTags(jedis.hget(infoKey, FIELD_TAGS))

            val pipeline = jedis.pipelined()
            pipeline.sadd(PROXIES_SET_KEY, proxyName)
            pipeline.hset(
                infoKey,
                mapOf(
                    FIELD_CAPACITY to proxyCapacity.toString(),
                    FIELD_LAST_SEEN to System.currentTimeMillis().toString(),
                    FIELD_ACCEPTING_PLAYERS to acceptingPlayers.toString(),
                    FIELD_FLEET_NAME to fleetName.orEmpty(),
                    FIELD_TAGS to tags.joinToString(","),
                ),
            )
            previousTags.forEach { tag -> pipeline.srem(PROXIES_BY_TAG_SET_KEY(tag), proxyName) }
            tags.forEach { tag -> pipeline.sadd(PROXIES_BY_TAG_SET_KEY(tag), proxyName) }
            pipeline.sync()
        }
    }

    override fun unregisterProxy(proxyName: String) {
        this.jedisPool.resource.use { jedis ->
            val infoKey = PROXIES_INFO_HASH_KEY(proxyName)
            val playersKey = PROXIES_PLAYERS_SET_KEY(proxyName)
            val previousTags = parseTags(jedis.hget(infoKey, FIELD_TAGS))
            val playersOnProxy = jedis.smembers(playersKey)

            // Players lose their whole position when the proxy they're on dies -
            // unlike a server disappearing, there is no connection left to salvage.
            val serverLookup = jedis.pipelined()
            val serverResponses = playersOnProxy.associateWith { serverLookup.hget(PLAYERS_HASH_KEY(it), FIELD_SERVER) }
            serverLookup.sync()

            val pipeline = jedis.pipelined()
            pipeline.srem(PROXIES_SET_KEY, proxyName)
            pipeline.del(infoKey)
            pipeline.del(playersKey)
            previousTags.forEach { tag -> pipeline.srem(PROXIES_BY_TAG_SET_KEY(tag), proxyName) }
            playersOnProxy.forEach { playerId ->
                pipeline.srem(PLAYERS_SET_KEY, playerId)
                pipeline.del(PLAYERS_HASH_KEY(playerId))
                serverResponses[playerId]?.get()?.let { serverName ->
                    pipeline.srem(SERVERS_PLAYERS_SET_KEY(serverName), playerId)
                }
            }
            pipeline.sync()

            playersOnProxy.forEach { jedis.publish(PLAYER_POSITION_CHANGED_CHANNEL, it) }
        }
    }

    override fun updateProxyLastSeen(proxyName: String) {
        this.jedisPool.resource.use { jedis ->
            if (!jedis.sismember(PROXIES_SET_KEY, proxyName)) {
                return
            }
            jedis.hset(PROXIES_INFO_HASH_KEY(proxyName), FIELD_LAST_SEEN, System.currentTimeMillis().toString())
        }
    }

    override fun updateProxyAcceptingPlayers(
        proxyName: String,
        acceptingPlayers: Boolean,
    ) {
        this.jedisPool.resource.use { jedis ->
            if (!jedis.sismember(PROXIES_SET_KEY, proxyName)) {
                return
            }
            jedis.hset(PROXIES_INFO_HASH_KEY(proxyName), FIELD_ACCEPTING_PLAYERS, acceptingPlayers.toString())
        }
    }

    override fun listRegisteredProxies(): List<RegisteredProxy> {
        this.jedisPool.resource.use { jedis ->
            return jedis.smembers(PROXIES_SET_KEY).mapNotNull { buildRegisteredProxy(jedis, it) }
        }
    }

    override fun getProxy(proxyName: String): Optional<RegisteredProxy> {
        this.jedisPool.resource.use { jedis ->
            return Optional.ofNullable(buildRegisteredProxy(jedis, proxyName))
        }
    }

    override fun listProxiesByTag(tag: String): List<RegisteredProxy> {
        this.jedisPool.resource.use { jedis ->
            return jedis.smembers(PROXIES_BY_TAG_SET_KEY(tag)).mapNotNull { buildRegisteredProxy(jedis, it) }
        }
    }

    override fun tryLockLostProxiesPurgeTask(ownerProxyName: String): Optional<CacheAdapter.Lock> =
        this.tryLock(ownerProxyName, LOCKS_LOST_PROXIES_PURGE_KEY, PROXY_LOST_PURGE_LOCK_SECONDS)

    // ==================== Servers ====================

    override fun registerServer(
        name: String,
        fleetName: String?,
        tags: List<String>,
        maxPlayers: Int,
        acceptingPlayers: Boolean?,
        address: String?,
        source: String,
    ) {
        this.jedisPool.resource.use { jedis ->
            val infoKey = SERVERS_INFO_HASH_KEY(name)
            val existing = jedis.hgetAll(infoKey)
            val existingSource = existing[FIELD_SOURCE]

            if (
                existingSource != null &&
                source == CacheAdapter.SOURCE_EXTERNAL_DYNAMIC &&
                existingSource != CacheAdapter.SOURCE_EXTERNAL_DYNAMIC
            ) {
                return
            }

            val previousTags = parseTags(existing[FIELD_TAGS])
            val existingCapacity = existing[FIELD_CAPACITY]?.toIntOrNull() ?: 0
            val effectiveCapacity = if (maxPlayers > 0) maxPlayers else existingCapacity
            val effectiveAccepting =
                acceptingPlayers
                    ?: existing[FIELD_ACCEPTING_PLAYERS]?.toBooleanStrictOrNull()
                    ?: true
            val effectiveFleet = fleetName ?: existing[FIELD_FLEET_NAME]?.takeIf { it.isNotBlank() }
            val effectiveTags = tags.ifEmpty { previousTags }
            val effectiveAddress = address ?: existing[FIELD_ADDRESS]

            val fields =
                mutableMapOf(
                    FIELD_CAPACITY to effectiveCapacity.toString(),
                    FIELD_FLEET_NAME to effectiveFleet.orEmpty(),
                    FIELD_TAGS to effectiveTags.joinToString(","),
                    FIELD_ACCEPTING_PLAYERS to effectiveAccepting.toString(),
                    FIELD_SOURCE to source,
                    FIELD_LAST_UPDATED to System.currentTimeMillis().toString(),
                )
            if (effectiveAddress != null) {
                fields[FIELD_ADDRESS] = effectiveAddress
            }

            val pipeline = jedis.pipelined()
            pipeline.sadd(SERVERS_SET_KEY, name)
            if (source == CacheAdapter.SOURCE_EXTERNAL_DYNAMIC) {
                pipeline.sadd(SERVERS_EXTERNAL_DYNAMIC_SET_KEY, name)
            } else {
                pipeline.srem(SERVERS_EXTERNAL_DYNAMIC_SET_KEY, name)
            }
            pipeline.hset(infoKey, fields)
            previousTags.forEach { tag -> pipeline.srem(SERVERS_BY_TAG_SET_KEY(tag), name) }
            effectiveTags.forEach { tag -> pipeline.sadd(SERVERS_BY_TAG_SET_KEY(tag), name) }
            pipeline.sync()

            jedis.publish(SERVER_UPDATED_CHANNEL, name)
        }
    }

    override fun unregisterServer(serverName: String) {
        this.jedisPool.resource.use { jedis ->
            val infoKey = SERVERS_INFO_HASH_KEY(serverName)
            val previousTags = parseTags(jedis.hget(infoKey, FIELD_TAGS))

            // Unlike a dead proxy, a server disappearing doesn't necessarily mean its
            // players are gone (Velocity may already have moved them elsewhere) - we
            // only drop the now-unused reverse index, we don't touch player state.
            val pipeline = jedis.pipelined()
            pipeline.srem(SERVERS_SET_KEY, serverName)
            pipeline.srem(SERVERS_EXTERNAL_DYNAMIC_SET_KEY, serverName)
            previousTags.forEach { tag -> pipeline.srem(SERVERS_BY_TAG_SET_KEY(tag), serverName) }
            pipeline.del(infoKey)
            pipeline.del(SERVERS_PLAYERS_SET_KEY(serverName))
            pipeline.sync()

            jedis.publish(SERVER_UPDATED_CHANNEL, serverName)
        }
    }

    override fun updateServerAcceptingPlayers(
        name: String,
        acceptingPlayers: Boolean,
    ) {
        this.jedisPool.resource.use { jedis ->
            val infoKey = SERVERS_INFO_HASH_KEY(name)
            if (!jedis.exists(infoKey)) {
                return
            }
            jedis.hset(
                infoKey,
                mapOf(
                    FIELD_ACCEPTING_PLAYERS to acceptingPlayers.toString(),
                    FIELD_LAST_UPDATED to System.currentTimeMillis().toString(),
                ),
            )
            jedis.publish(SERVER_UPDATED_CHANNEL, name)
        }
    }

    override fun updateServerMaxPlayers(
        name: String,
        maxPlayers: Int,
    ) {
        this.jedisPool.resource.use { jedis ->
            val infoKey = SERVERS_INFO_HASH_KEY(name)
            if (!jedis.exists(infoKey)) {
                return
            }
            jedis.hset(infoKey, FIELD_CAPACITY, maxPlayers.toString())
        }
    }

    override fun listAllServers(): List<RegisteredServer> {
        this.jedisPool.resource.use { jedis ->
            return jedis.smembers(SERVERS_SET_KEY).mapNotNull { buildRegisteredServer(jedis, it) }
        }
    }

    override fun listServersByTag(tag: String): List<RegisteredServer> {
        this.jedisPool.resource.use { jedis ->
            return jedis.smembers(SERVERS_BY_TAG_SET_KEY(tag)).mapNotNull { buildRegisteredServer(jedis, it) }
        }
    }

    override fun getServer(name: String): Optional<RegisteredServer> {
        this.jedisPool.resource.use { jedis ->
            return Optional.ofNullable(buildRegisteredServer(jedis, name))
        }
    }

    override fun registerExternalServer(
        name: String,
        address: String,
        tags: List<String>,
        maxPlayers: Int,
    ): Boolean {
        this.jedisPool.resource.use { jedis ->
            val existingSource = jedis.hget(SERVERS_INFO_HASH_KEY(name), FIELD_SOURCE)
            if (
                existingSource != null &&
                existingSource != CacheAdapter.SOURCE_EXTERNAL_DYNAMIC
            ) {
                return false
            }
        }

        this.registerServer(
            name = name,
            fleetName = null,
            tags = tags,
            maxPlayers = maxPlayers,
            acceptingPlayers = true,
            address = address,
            source = CacheAdapter.SOURCE_EXTERNAL_DYNAMIC,
        )

        this.jedisPool.resource.use { jedis ->
            jedis.publish(EXTERNAL_SERVER_REGISTER_CHANNEL, name)
        }
        return true
    }

    override fun unregisterExternalServer(name: String): Boolean {
        this.jedisPool.resource.use { jedis ->
            val source = jedis.hget(SERVERS_INFO_HASH_KEY(name), FIELD_SOURCE)
            if (source != CacheAdapter.SOURCE_EXTERNAL_DYNAMIC) {
                return false
            }
        }

        this.unregisterServer(name)
        this.jedisPool.resource.use { jedis ->
            jedis.publish(EXTERNAL_SERVER_UNREGISTER_CHANNEL, name)
        }
        return true
    }

    override fun listExternalDynamicServers(): List<RegisteredServer> {
        this.jedisPool.resource.use { jedis ->
            return jedis.smembers(SERVERS_EXTERNAL_DYNAMIC_SET_KEY).mapNotNull { buildRegisteredServer(jedis, it) }
        }
    }

    // ==================== Players ====================

    override fun listPlayersInServer(serverName: String): List<UUID> {
        this.jedisPool.resource.use { jedis ->
            return jedis.smembers(SERVERS_PLAYERS_SET_KEY(serverName)).map(UUID::fromString)
        }
    }

    override fun setPlayerPosition(
        playerId: UUID,
        proxyName: String,
        serverName: String,
    ) {
        val playerIdStr = playerId.toString()
        val playerKey = PLAYERS_HASH_KEY(playerIdStr)

        this.jedisPool.resource.use { jedis ->
            val previous = jedis.hgetAll(playerKey)
            val previousProxy = previous[FIELD_PROXY]
            val previousServer = previous[FIELD_SERVER]

            val pipeline = jedis.pipelined()
            pipeline.sadd(PLAYERS_SET_KEY, playerIdStr)
            pipeline.hset(
                playerKey,
                mapOf(
                    FIELD_PROXY to proxyName,
                    FIELD_SERVER to serverName,
                    FIELD_LAST_SEEN to System.currentTimeMillis().toString(),
                ),
            )
            if (previousProxy != null && previousProxy != proxyName) {
                pipeline.srem(PROXIES_PLAYERS_SET_KEY(previousProxy), playerIdStr)
            }
            pipeline.sadd(PROXIES_PLAYERS_SET_KEY(proxyName), playerIdStr)
            if (previousServer != null && previousServer != serverName) {
                pipeline.srem(SERVERS_PLAYERS_SET_KEY(previousServer), playerIdStr)
            }
            pipeline.sadd(SERVERS_PLAYERS_SET_KEY(serverName), playerIdStr)
            pipeline.sync()

            jedis.publish(PLAYER_POSITION_CHANGED_CHANNEL, playerIdStr)
        }
    }

    override fun unsetPlayerPosition(playerId: UUID) {
        val playerIdStr = playerId.toString()
        val playerKey = PLAYERS_HASH_KEY(playerIdStr)

        this.jedisPool.resource.use { jedis ->
            val previous = jedis.hgetAll(playerKey)
            if (previous.isEmpty()) {
                return
            }

            val pipeline = jedis.pipelined()
            pipeline.srem(PLAYERS_SET_KEY, playerIdStr)
            pipeline.del(playerKey)
            previous[FIELD_PROXY]?.let { pipeline.srem(PROXIES_PLAYERS_SET_KEY(it), playerIdStr) }
            previous[FIELD_SERVER]?.let { pipeline.srem(SERVERS_PLAYERS_SET_KEY(it), playerIdStr) }
            pipeline.sync()

            jedis.publish(PLAYER_POSITION_CHANGED_CHANNEL, playerIdStr)
        }
    }

    override fun getPlayerPosition(playerId: UUID): Optional<PlayerPosition> {
        this.jedisPool.resource.use { jedis ->
            val info = jedis.hgetAll(PLAYERS_HASH_KEY(playerId.toString()))
            val proxyName = info[FIELD_PROXY]
            val serverName = info[FIELD_SERVER]
            if (proxyName.isNullOrBlank() || serverName.isNullOrBlank()) {
                return Optional.empty()
            }
            return Optional.of(PlayerPosition(proxyName, serverName))
        }
    }

    override fun isPlayerConnected(playerId: UUID): Boolean {
        this.jedisPool.resource.use { jedis ->
            return jedis.sismember(PLAYERS_SET_KEY, playerId.toString())
        }
    }

    override fun updateCachedPlayerName(
        playerId: UUID,
        playerName: String,
    ) {
        this.jedisPool.resource.use { jedis ->
            val playerIdString = playerId.toString()
            val params = SetParams().ex(PLAYER_ID_CACHE_TTL_SECONDS)

            val pipeline = jedis.pipelined()
            pipeline.set(UUID_CACHE_ID_TO_NAME_KEY(playerIdString), playerName, params)
            pipeline.set(UUID_CACHE_NAME_TO_ID_KEY(playerName), playerIdString, params)
            pipeline.sync()
        }
    }

    override fun getPlayerNameFromId(playerId: UUID): Optional<String> {
        this.jedisPool.resource.use { jedis ->
            return Optional.ofNullable(jedis.get(UUID_CACHE_ID_TO_NAME_KEY(playerId.toString())))
        }
    }

    override fun getPlayerIdFromName(playerName: String): Optional<UUID> {
        this.jedisPool.resource.use { jedis ->
            return Optional.ofNullable(jedis.get(UUID_CACHE_NAME_TO_ID_KEY(playerName))).map(UUID::fromString)
        }
    }

    override fun getPlayerNamesFromIds(playerIds: List<UUID>): Map<UUID, String> {
        this.jedisPool.resource.use { jedis ->
            val pipeline = jedis.pipelined()
            val responses = playerIds.associateWith { uuid -> pipeline.get(UUID_CACHE_ID_TO_NAME_KEY(uuid.toString())) }
            pipeline.sync()

            return responses.mapNotNull { (uuid, response) ->
                val name = response.get() ?: return@mapNotNull null
                uuid to name
            }.toMap()
        }
    }

    override fun countOnlinePlayers(): Int {
        this.jedisPool.resource.use { jedis ->
            return jedis.scard(PLAYERS_SET_KEY).toInt()
        }
    }

    override fun countPlayerCapacity(): Int {
        this.jedisPool.resource.use { jedis ->
            return jedis.smembers(PROXIES_SET_KEY).sumOf { proxyName ->
                jedis.hget(PROXIES_INFO_HASH_KEY(proxyName), FIELD_CAPACITY)?.toIntOrNull() ?: 0
            }
        }
    }

    // ==================== Builders ====================

    private fun buildRegisteredProxy(
        jedis: Jedis,
        name: String,
    ): RegisteredProxy? {
        if (!jedis.sismember(PROXIES_SET_KEY, name)) {
            return null
        }

        val info = jedis.hgetAll(PROXIES_INFO_HASH_KEY(name))
        if (info.isEmpty()) {
            return null
        }

        return RegisteredProxy(
            name,
            info[FIELD_CAPACITY]?.toIntOrNull() ?: 0,
            info[FIELD_LAST_SEEN]?.toLongOrNull()?.let(Instant::ofEpochMilli) ?: Instant.now(),
            info[FIELD_FLEET_NAME]?.takeIf { it.isNotBlank() },
            parseTags(info[FIELD_TAGS]),
            info[FIELD_ACCEPTING_PLAYERS]?.toBooleanStrictOrNull() ?: true,
        )
    }

    private fun buildRegisteredServer(
        jedis: Jedis,
        name: String,
    ): RegisteredServer? {
        val info = jedis.hgetAll(SERVERS_INFO_HASH_KEY(name))
        if (info.isEmpty()) {
            return null
        }

        val source = info[FIELD_SOURCE] ?: CacheAdapter.SOURCE_MANAGED

        return RegisteredServer(
            name,
            info[FIELD_FLEET_NAME]?.takeIf { it.isNotBlank() },
            parseTags(info[FIELD_TAGS]),
            info[FIELD_ADDRESS]?.takeIf { it.isNotBlank() },
            info[FIELD_CAPACITY]?.toIntOrNull() ?: 0,
            jedis.scard(SERVERS_PLAYERS_SET_KEY(name)).toInt(),
            info[FIELD_ACCEPTING_PLAYERS]?.toBooleanStrictOrNull() ?: true,
            source != CacheAdapter.SOURCE_MANAGED,
            info[FIELD_LAST_UPDATED]?.toLongOrNull()?.let(Instant::ofEpochMilli) ?: Instant.now(),
        )
    }

    private fun parseTags(raw: String?): List<String> =
        raw
            ?.split(",")
            ?.map { it.trim() }
            ?.filter { it.isNotEmpty() }
            ?: emptyList()

    private fun tryLock(
        ownerProxyName: String,
        key: String,
        ttlSeconds: Long,
    ): Optional<CacheAdapter.Lock> {
        this.jedisPool.resource.use { jedis ->
            val success = jedis.set(key, ownerProxyName, SetParams().nx().ex(ttlSeconds)) != null

            if (success) {
                return Optional.of(
                    object : CacheAdapter.Lock {
                        override fun close() {
                            jedisPool.resource.use { jedis -> jedis.del(key) }
                        }
                    },
                )
            }

            return Optional.empty()
        }
    }
}
