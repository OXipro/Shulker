package io.shulkermc.cluster.api

import redis.clients.jedis.DefaultJedisClientConfig
import redis.clients.jedis.HostAndPort
import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig
import java.time.Duration
import java.util.Optional

/**
 * Redis connection details, shared by every [ShulkerClusterAPI]
 * implementation (in-cluster or standalone) since none of the Redis
 * registry logic (see [io.shulkermc.cluster.api.adapters.cache.RedisCacheAdapter])
 * depends on how the process identifies itself.
 */
@SuppressWarnings("detekt:MagicNumber")
data class RedisConfiguration(
    val host: String,
    val port: Int,
    val username: Optional<String>,
    val password: Optional<String>,
) {
    fun createJedisPool(): JedisPool {
        val poolConfig =
            JedisPoolConfig().apply {
                maxTotal = 16
                maxIdle = 8
                minIdle = 1
                testOnBorrow = true
                testWhileIdle = true
                setMaxWait(Duration.ofSeconds(3))
            }

        val clientConfigBuilder =
            DefaultJedisClientConfig.builder()
                .connectionTimeoutMillis(3_000)
                .socketTimeoutMillis(3_000)

        // Password-only (ACL default user) and user+password are both valid.
        if (this.username.isPresent) {
            clientConfigBuilder.user(this.username.get())
        }
        if (this.password.isPresent) {
            clientConfigBuilder.password(this.password.get())
        }

        return JedisPool(
            poolConfig,
            HostAndPort(this.host, this.port),
            clientConfigBuilder.build(),
        )
    }
}
