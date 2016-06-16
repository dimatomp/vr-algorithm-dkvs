package net.dimatomp.parallel.vr.dkvs

/**
 * Created by dimatomp on 13.06.16.
 */
class ViewChange(val prevView: Int, var primaryCandidate: Int, var latestLog: Log, var opNumber: Long, var commitNumber: Long, var chosenBy: Int = 0): Status

fun VRMessageProcessor.processStartViewChange(m: StartViewChange) {
    var status = status
    when (status) {
        is Normal ->
            if (m.view > viewNumber)
                status = ViewChange(viewNumber, m.initiator, log, opNumber, commitNumber)
            else
                return
        is ViewChange ->
            if (m.view > viewNumber)
                status = ViewChange(status.prevView, m.initiator, log, status.opNumber, status.commitNumber)
            else
                return
        else -> throw IllegalStateException("View change during recovery not supported")
    }
    this.status = status
    viewNumber = m.view
    broker.sendMessage(DoViewChange(viewNumber, log, status.prevView, opNumber, commitNumber), Replica(status.primaryCandidate))
}

fun VRMessageProcessor.processDoViewChange(m: DoViewChange) {
    var status = status
    when (status) {
        is Normal ->
            if (m.view > viewNumber)
                status = ViewChange(viewNumber, replicaId, log, opNumber, commitNumber, 1)
            else
                return
        is ViewChange ->
            if (m.view > viewNumber)
                status = ViewChange(status.prevView, replicaId, status.latestLog, status.opNumber, status.commitNumber, 1)
        else -> throw IllegalStateException("View change during recovery not supported")
    }
    this.status = status
    viewNumber = m.view
    if (m.viewBeforeChange > status.prevView || (m.viewBeforeChange == status.prevView && m.opNumber >= status.opNumber)) {
        status.latestLog = m.log
        status.opNumber = m.opNumber
        if (m.viewBeforeChange == status.prevView)
            status.commitNumber = Math.max(status.commitNumber, m.commitNumber)
        else
            status.commitNumber = m.commitNumber
    }
    if (++status.chosenBy > numReplicas / 2) {
        this.status = NormalPrimary()
        this.log = status.latestLog
        this.opNumber = status.opNumber
        this.commitNumber = status.commitNumber
        broadcast { broker.sendMessage(StartView(viewNumber, log, replicaId, opNumber, commitNumber), Replica(it)) }
    }
}

fun VRMessageProcessor.processStartView(m: StartView) {
    if (status is ViewChange && m.view == viewNumber || m.view > viewNumber) {
        status = NormalBackup(m.primaryId)
        log = m.log
        commitNumber = m.commitNumber
        opNumber = m.opNumber
    }
}
