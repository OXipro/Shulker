package io.shulkermc.proxy.tasks

import io.shulkermc.proxy.ProxyInterface
import io.shulkermc.proxy.ShulkerProxyAgentCommon
import java.util.concurrent.TimeUnit
import java.util.logging.Level

class HealthcheckTask(private val agent: ShulkerProxyAgentCommon) : Runnable {
    companion object {
        private const val HEALTHCHECK_INTERVAL_SECONDS = 5L
    }

    fun schedule(): ProxyInterface.ScheduledTask {
        return this.agent.proxyInterface.scheduleRepeatingTask(
            0L,
            HEALTHCHECK_INTERVAL_SECONDS,
            TimeUnit.SECONDS,
            this,
        )
    }

    override fun run() {
        try {
            this.agent.cluster.agonesGateway.sendHealthcheck()
        } catch (
            @Suppress("TooGenericExceptionCaught") e: Exception,
        ) {
            this.agent.logger.log(Level.WARNING, "Failed to send Agones healthcheck", e)
        }

        try {
            this.agent.cluster.cache.updateProxyLastSeen(this.agent.cluster.selfReference.name)
        } catch (
            @Suppress("TooGenericExceptionCaught") e: Exception,
        ) {
            this.agent.logger.log(Level.WARNING, "Failed to update proxy last-seen in Redis", e)
        }
    }
}
