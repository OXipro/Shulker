package io.shulkermc.cluster.api

import redis.clients.jedis.DefaultJedisClientConfig
import redis.clients.jedis.HostAndPort
import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig
import java.time.Duration
import java.util.Optional
import kotlin.jvm.optionals.getOrDefault

@SuppressWarnings("detekt:MagicNumber")
data class Configuration(
    val clusterName: String,
    val owningFleetName: Optional<String>,
    val redis: Redis,
) {
    data class Redis(
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

    companion object {
        fun fromEnvironment(): Configuration {
            return Configuration(
                clusterName = requireNotNull(System.getenv("SHULKER_CLUSTER_NAME")) { "Missing SHULKER_CLUSTER_NAME" },
                owningFleetName = Optional.ofNullable(System.getenv("SHULKER_OWNING_FLEET_NAME")),
                redis =
                    Redis(
                        host = requireNotNull(System.getenv("SHULKER_REDIS_HOST")) { "Missing SHULKER_REDIS_HOST" },
                        port =
                            Optional.ofNullable(System.getenv("SHULKER_REDIS_PORT"))
                                .map { it.toInt() }
                                .getOrDefault(6379),
                        username = Optional.ofNullable(System.getenv("SHULKER_REDIS_USERNAME")),
                        password = Optional.ofNullable(System.getenv("SHULKER_REDIS_PASSWORD")),
                    ),
            )
        }
    }
}
