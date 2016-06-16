package net.dimatomp.parallel.vr.client

import net.dimatomp.parallel.vr.api.MessageBroker
import net.dimatomp.parallel.vr.api.MessageProcessor
import net.dimatomp.parallel.vr.dkvs.*

/**
 * Created by dimatomp on 13.06.16.
 */
class ClientMessageProcessor(broker: MessageBroker<Message, Address>, var replicaId: Int = 2): MessageProcessor<Message, Address>(broker) {
    private var opNumber: Long = 0

    override fun onMessage(m: Message) {
        println(when (m) {
            is Value -> "VALUE ${m.key} ${m.value}"
            is NotFound -> "NOT FOUND"
            is Stored -> "STORED"
            is Deleted -> "DELETED"
            is Pong -> "PONG"
            else -> "Unknown server response"
        })
    }

    fun parseAndSend(command: String) {
        val parts = command.split(' ')
        val request = when (parts[0]) {
            "GET" -> Get(parts[1])
            "SET" -> Set(parts[1], parts[2])
            "DELETE" -> Delete(parts[1])
            "PING" -> Ping()
            else -> null
        }
        if (request != null)
            broker.sendMessage(Request(request, broker.myAddress as Client, opNumber++), Replica(replicaId))
    }
}
