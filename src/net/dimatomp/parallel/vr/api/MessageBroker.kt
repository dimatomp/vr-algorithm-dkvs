package net.dimatomp.parallel.vr.api

/**
 * Created by dimatomp on 22.05.16.
 */
interface MessageBroker<M, A> {
    val myAddress: A

    fun registerClient(client: MessageProcessor<M, A>)

    fun sendMessage(m: M, recipient: A)

    enum class Interval {
        SHORT, LONG
    }

    fun scheduleRepeated(interval: Interval, operation: () -> Unit)
    fun cancelScheduled()
}
