package io.shulkermc.server.api

import io.shulkermc.server.ShulkerServerAgentCommon
import java.util.concurrent.CompletableFuture

class ShulkerServerAPIImpl(private val agent: ShulkerServerAgentCommon) : ShulkerServerAPI() {
    override fun askShutdown() = this.agent.shutdown()

    override fun setReady(): CompletableFuture<Void> = this.agent.cluster.agonesGateway.setReady().thenAccept {}

    override fun setAllocated(): CompletableFuture<Void> = this.agent.cluster.agonesGateway.setAllocated().thenAccept {}

    override fun setReserved(seconds: Long): CompletableFuture<Void> =
        this.agent.cluster.agonesGateway.setReserved(seconds).thenAccept {}

    override fun setAcceptingPlayers(acceptingPlayers: Boolean) {
        this.agent.cluster.cache.updateServerAcceptingPlayers(
            this.agent.cluster.selfReference.name,
            acceptingPlayers,
        )
    }
}
