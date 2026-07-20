package io.shulkermc.cluster.api

import java.util.Optional
import kotlin.jvm.optionals.getOrDefault

data class Configuration(
    val clusterName: String,
    val owningFleetName: Optional<String>,
    val redis: RedisConfiguration,
) {
    companion object {
        fun fromEnvironment(): Configuration {
            return Configuration(
                clusterName = requireNotNull(System.getenv("SHULKER_CLUSTER_NAME")) { "Missing SHULKER_CLUSTER_NAME" },
                owningFleetName = Optional.ofNullable(System.getenv("SHULKER_OWNING_FLEET_NAME")),
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
            )
        }
    }
}
