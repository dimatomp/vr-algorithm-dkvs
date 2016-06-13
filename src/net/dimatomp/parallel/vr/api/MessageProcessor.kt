package net.dimatomp.parallel.vr.api

/**
 * Created by dimatomp on 22.05.16.
 */
abstract class MessageProcessor<M, A>(val broker: MessageBroker<M, A>) {
    init {
        broker.registerClient(this)
    }

    abstract fun onMessage(m: M)
}