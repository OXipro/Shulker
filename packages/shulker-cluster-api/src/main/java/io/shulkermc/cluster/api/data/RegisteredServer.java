package io.shulkermc.cluster.api.data;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.util.List;

/**
 * Snapshot of a Minecraft server registered in the Shulker cluster cache.
 * Includes fleet-managed GameServers and external servers.
 *
 * @param name Server identifier as known by proxies
 * @param fleetName Owning MinecraftServerFleet name, if any
 * @param tags Tags associated with the server (e.g. lobby, limbo)
 * @param address Optional host:port of the server
 * @param maxPlayers Per-instance capacity from MinecraftServer/Fleet {@code spec.config.maxPlayers}
 *                   (stored in Redis like proxy capacity: {@code shulker:servers:capacity})
 * @param currentPlayers Number of players currently on this server
 * @param acceptingPlayers Whether the server accepts new players (not liquidating / draining)
 * @param external Whether this server is external (not managed as a Shulker GameServer)
 * @param lastUpdated Last time metadata was written to the cache
 */
public record RegisteredServer(
        @NotNull String name,
        @Nullable String fleetName,
        @NotNull List<@NotNull String> tags,
        @Nullable String address,
        int maxPlayers,
        int currentPlayers,
        boolean acceptingPlayers,
        boolean external,
        @NotNull Instant lastUpdated
) {}
