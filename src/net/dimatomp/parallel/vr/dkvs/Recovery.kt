package net.dimatomp.parallel.vr.dkvs

import java.util.*

/**
 * Created by dimatomp on 17.06.16.
 */
class NodeRecovery(val id: Int, var primaryInfo: PrimaryInfo? = null, var nResponses: Int = 0): Status

fun VRMessageProcessor.processRecovery(m: Recovery) {
    when (status) {
        is NormalPrimary -> broker.sendMessage(
                RecoveryResponse(viewNumber, m.id, PrimaryInfo(replicaId, Log(ArrayList(log.userReqs.subList(m.numOps.toInt(), log.userReqs.size))), opNumber, commitNumber)),
                Replica(m.replicaId))
        is NormalBackup -> broker.sendMessage(RecoveryResponse(viewNumber, m.id, null), Replica(m.replicaId))
        else -> pendingMessages.add(m)
    }
}

fun VRMessageProcessor.processRecoveryResponse(m: RecoveryResponse) {
    val status = status
    if (status !is NodeRecovery || m.id != status.id)
        return
    if (m.view >= viewNumber) {
        if (m.view > viewNumber)
            status.primaryInfo = null
        viewNumber = m.view
        if (m.primaryInfo != null)
            status.primaryInfo = m.primaryInfo
    }
    if (++status.nResponses > numReplicas / 2 && status.primaryInfo != null) {
        val primaryInfo = status.primaryInfo!!
        primaryInfo.log.userReqs.forEach { respondMessage(it) }
        opNumber = primaryInfo.opNumber
        commitNumber = primaryInfo.commitNumber
        this.status = NormalBackup(primaryInfo.primaryId)
    }
}