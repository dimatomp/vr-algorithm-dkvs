package net.dimatomp.parallel.vr.dkvs

import net.dimatomp.parallel.vr.api.MessageBroker
import net.dimatomp.parallel.vr.api.MessageProcessor
import java.util.*

/**
 * Created by dimatomp on 22.05.16.
 */
class VRMessageProcessor(val numReplicas: Int, val replicaId: Int, broker: MessageBroker<Message, Address>):
        MessageProcessor<Message, Address>(broker) {
    var viewNumber: Int = 0
    val pendingMessages = ArrayDeque<Message>()

    fun broadcast(op: (Int) -> Unit) {
        for (i in 1..numReplicas)
            if (i != replicaId)
                op(i)
    }

    private fun processPendingMessages() {
        val messages = pendingMessages.toList()
        pendingMessages.clear()
        messages.forEach { broker.sendMessage(it, Replica(replicaId)) }
    }

    private fun changeStatus(value: Status) {
        if (value !is Normal)
            throw IllegalStateException("View changes and recovery not supported")
        processPendingMessages()
        broker.cancelScheduled()
        when (value) {
            is NormalPrimary -> broker.scheduleRepeated(MessageBroker.Interval.SHORT) {
                val message = Commit(viewNumber, commitNumber)
                broadcast { broker.sendMessage(message, Replica(it)) }
            }
            is NormalBackup -> broker.scheduleRepeated(MessageBroker.Interval.LONG) {
                if (value.triggered)
                    value.triggered = false
                else
                    status = ViewChange
            }
        }
    }
    var status: Status = if (replicaId == 1) NormalPrimary() else NormalBackup(1)
        set(value) { changeStatus(value); field = value }
    init {
        changeStatus(status)
    }

    var opNumber: Long = 0
    var log: Log = Log(ArrayList<Request>())
    var commitNumber: Long = 0
    val clientTable = HashMap<Client, LastRequest>()

    private val data = HashMap<String, String>()

    fun respondMessage(m: Request): Response = when (m.op) {
        is Get -> {
            val result = data[m.op.key]
            if (result == null) NotFound() else Value(m.op.key, result)
        }
        is Set -> {
            data[m.op.key] = m.op.value
            Stored()
        }
        is Delete -> {
            data.remove(m.op.key)
            Deleted()
        }
        is Ping -> Pong()
        else -> throw UnsupportedOperationException("Unknown requested operation: ${m.op}")
    }

    override fun onMessage(m: Message) {
        return when (m) {
            is Request -> processRequest(m)
            is Prepare -> processPrepare(m)
            is PrepareOk -> processPrepareOk(m)
            is Commit -> processCommit(m)
            else -> throw UnsupportedOperationException("Operation $m not supported")
        }
    }
}
