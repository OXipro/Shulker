package io.shulkermc.cluster.api.standalone

import io.shulkermc.cluster.api.RedisConfiguration
import java.net.InetAddress
import java.util.Optional
import java.util.UUID
import kotlin.jvm.optionals.getOrDefault

/**
 * Configuration for [io.shulkermc.cluster.api.ShulkerClusterAPIStandalone].
 * Unlike the in-cluster [io.shulkermc.cluster.api.ShulkerClusterAPIImpl],
 * there is no Agones sidecar and no Kubernetes API to identify "who am I" -
 * the only thing needed is a Redis endpoint reachable from wherever this
 * process runs, plus an identity string used for targeted pub/sub messages
 * (see [io.shulkermc.cluster.api.adapters.pubsub.RedisPubSubAdapter]).
 */
data class StandaloneConfiguration(
    val redis: RedisConfiguration,
    val clientName: String,
) {
    companion object {
        /**
         * Reads configuration from the same `SHULKER_REDIS_*` environment
         * variables used by the in-cluster agents, plus
         * `SHULKER_STANDALONE_CLIENT_NAME` (defaults to the local hostname,
         * falling back to a random identifier if it cannot be resolved).
         */
        fun fromEnvironment(): StandaloneConfiguration {
            return StandaloneConfiguration(
                redis =
                    RedisConfiguration(
                        host = requireNotNull(System.getenv("SHULKER_REDIS_HOST")) { "Missing SHULKER_REDIS_HOST" },
                        port =
                            Optional.ofNullable(System.getenv("SHULKER_REDIS_PORT"))
                                .map { it.toInt() }
                                .getOrDefault(6379),
                        username = Optional.ofNullable(System.getenv("SHULKER_REDIS_USERNAME")),
                        password = Optional.ofNullable(System.getenv("SHULKER_REDIS_PASSWORD")),
                    ),
                clientName =
                    Optional.ofNullable(System.getenv("SHULKER_STANDALONE_CLIENT_NAME"))
                        .getOrDefault(defaultClientName()),
            )
        }

        private fun defaultClientName(): String =
            runCatching { InetAddress.getLocalHost().hostName }
                .getOrDefault("standalone-${UUID.randomUUID().toString().substring(0, 8)}")
    }
}
