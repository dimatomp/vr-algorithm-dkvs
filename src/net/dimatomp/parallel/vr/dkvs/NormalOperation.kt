package net.dimatomp.parallel.vr.dkvs

import java.util.*

/**
 * Created by dimatomp on 22.05.16.
 */
interface Normal: Status
class NormalPrimary: Normal {
    val clientRequests = TreeMap<Long, ClientRequest>()
}
class NormalBackup(val primaryId: Int): Normal {
    var triggered: Boolean = false
    val pendingPrepare = TreeMap<Long, Prepare>()
}

data class LastRequest(val request: Request, val response: Response)

data class ClientRequest(val request: Request, var response: Response?, var okReplies: Int = 0)

fun VRMessageProcessor.processRequest(m: Request) {
    var status: Status = status
    if (status !is Normal) {
        pendingMessages.add(m)
        return
    }
    if (status is NormalBackup) {
        broker.sendMessage(m, Replica(status.primaryId))
        return
    }
    status = status as NormalPrimary
    val lastRequest = clientTable[m.client]
    if (lastRequest == null || lastRequest.request.requestNum < m.requestNum) {
        log.userReqs.add(m)
        status.clientRequests[++opNumber] = ClientRequest(m, null)
        broadcast { broker.sendMessage(Prepare(viewNumber, m, opNumber, commitNumber), Replica(it)) }
    } else if (lastRequest.request.requestNum == m.requestNum)
        broker.sendMessage(lastRequest.response, m.client)
}

fun VRMessageProcessor.processPrepareOk(m: PrepareOk) {
    var status = status
    if (status !is Normal) {
        pendingMessages.add(m)
        return
    }
    if (m.view < viewNumber)
        return
    status = status as NormalPrimary
    val okReplies = ++status.clientRequests[m.opNumber]!!.okReplies
    if (okReplies == numReplicas / 2) {
        while (commitNumber < m.opNumber) {
            val current = status.clientRequests[++commitNumber]!!
            val response = respondMessage(current.request)
            current.response = response
            broker.sendMessage(response, current.request.client)
        }
    }
}

fun VRMessageProcessor.processPrepare(m: Prepare) {
    var status = status
    if (status !is Normal) {
        pendingMessages.add(m)
        return
    }
    if (m.view < viewNumber)
        return
    if (m.view > viewNumber) {
        throw UnsupportedOperationException("Unimplemented!")
    }
    status = status as NormalBackup
    status.pendingPrepare[m.opNumber] = m
    while (!status.pendingPrepare.isEmpty() && status.pendingPrepare.firstKey() == opNumber + 1) {
        val cMessage = status.pendingPrepare[++opNumber]!!
        log.userReqs.add(cMessage.clientMessage)
        clientTable[m.clientMessage.client] = LastRequest(cMessage.clientMessage, respondMessage(cMessage.clientMessage))
        broker.sendMessage(PrepareOk(viewNumber, cMessage.opNumber, replicaId), Replica(status.primaryId))
        status.pendingPrepare.remove(opNumber)
    }
}

fun VRMessageProcessor.processCommit(m: Commit) {
    val status = status
    if (status !is NormalBackup || m.view < viewNumber)
        return
    if (m.view > viewNumber)
        throw UnsupportedOperationException("Unimplemented!")
    commitNumber = m.commitNumber
    status.triggered = true
}

