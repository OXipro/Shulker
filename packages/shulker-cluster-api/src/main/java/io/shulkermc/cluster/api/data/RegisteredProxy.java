package io.shulkermc.cluster.api.data;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.util.List;

/**
 * Snapshot of a proxy registered in the Shulker cluster cache.
 *
 * @param name Proxy identifier (usually the GameServer / pod name)
 * @param capacity Maximum number of players this proxy accepts
 * @param lastSeenAt Last heartbeat written by the proxy agent
 * @param fleetName Owning ProxyFleet name, if any
 * @param tags Tags associated with the proxy
 * @param acceptingPlayers Whether the proxy currently accepts new connections
 */
public record RegisteredProxy(
        @NotNull String name,
        int capacity,
        @NotNull Instant lastSeenAt,
        @Nullable String fleetName,
        @NotNull List<@NotNull String> tags,
        boolean acceptingPlayers
) {}
