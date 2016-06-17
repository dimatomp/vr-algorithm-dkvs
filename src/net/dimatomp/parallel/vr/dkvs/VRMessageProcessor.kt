package net.dimatomp.parallel.vr.dkvs

import net.dimatomp.parallel.vr.api.MessageBroker
import net.dimatomp.parallel.vr.api.MessageProcessor
import java.io.*
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

    var opNumber: Long = 0
    var log: Log = Log(ArrayList<Request>())
    var commitNumber: Long = 0
    val clientTable = HashMap<Client, LastRequest>()
    var fileLog: ObjectOutputStream? = null
    private val data = HashMap<String, String>()

    private fun changeStatus(value: Status) {
        broker.cancelScheduled()
        when (value) {
            is NormalPrimary -> broker.scheduleRepeated(MessageBroker.Interval.SHORT) {
                val message = Commit(viewNumber, commitNumber)
                broadcast { broker.sendMessage(message, Replica(it)) }
            }
            is NormalBackup -> broker.scheduleRepeated(MessageBroker.Interval.LONG) {
                if (value.triggered)
                    value.triggered = false
                else {
                    status = ViewChange(viewNumber, replicaId, log, opNumber, commitNumber, 1)
                    viewNumber++
                    broadcast { broker.sendMessage(StartViewChange(viewNumber, replicaId), Replica(it)) }
                    println("Initiating view change, new number $viewNumber")
                }
            }
            is ViewChange -> broker.scheduleRepeated(MessageBroker.Interval.LONG) {
                status = ViewChange(viewNumber, replicaId, log, opNumber, commitNumber, 1)
                viewNumber++
                broadcast { broker.sendMessage(StartViewChange(viewNumber, replicaId), Replica(it)) }
                println("Re-initiating stale view change process, new number $viewNumber")
            }
            is NodeRecovery -> broadcast { broker.sendMessage(Recovery(replicaId, value.id, log.userReqs.size.toLong()), Replica(it)) }
        }
    }
    var status: Status = initStatus()
        set(value) {
            changeStatus(value);
            field = value;
            processPendingMessages()
        }

    companion object {
        private val rng = Random()
    }

    private fun initStatus(): Status {
        val file = File("node.$replicaId.log")
        val result = if (file.exists()) {
            println("Found a log, recovering the node")
            val objInput = ObjectInputStream(FileInputStream(file))
            try {
                while (true) {
                    val m = try {
                        objInput.readObject() as Request
                    } catch (e: EOFException) {
                        break
                    }
                    respondMessage(m)
                }
            } finally {
                objInput.close()
            }
            file.delete()
            // TODO Figure out if we can append here
            fileLog = ObjectOutputStream(FileOutputStream(file))
            for (m in log.userReqs)
                fileLog!!.writeObject(m)
            NodeRecovery(rng.nextInt())
        } else {
            fileLog = ObjectOutputStream(FileOutputStream(file))
            if (replicaId == 1)
                NormalPrimary()
            else
                NormalBackup(1)
        }
        changeStatus(result)
        return result
    }

    fun respondMessage(m: Request): Response {
        log.userReqs.add(m)
        fileLog?.writeObject(m)
        return when (m.op) {
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
    }

    override fun onMessage(m: Message) {
        return when (m) {
            is Request -> processRequest(m)
            is Prepare -> processPrepare(m)
            is PrepareOk -> processPrepareOk(m)
            is Commit -> processCommit(m)
            is StartViewChange -> processStartViewChange(m)
            is DoViewChange -> processDoViewChange(m)
            is StartView -> processStartView(m)
            is Recovery -> processRecovery(m)
            is RecoveryResponse -> processRecoveryResponse(m)
            else -> throw UnsupportedOperationException("Operation $m not supported")
        }
    }
}
