package net.dimatomp.parallel.vr.dkvs

import net.dimatomp.parallel.vr.broker.UDPMessageBroker
import java.util.*

/**
 * Created by dimatomp on 12.06.16.
 */
fun main(args: Array<String>) {
    val properties = Properties()
    val stream = ClassLoader.getSystemClassLoader().getResourceAsStream("dkvs.properties")
    properties.load(stream)
    stream.close()
    val replicaId = args[0].toInt()
    val broker = UDPMessageBroker(replicaId, properties)
    var number = 1
    while (properties.getProperty("node.$number") != null)
        number++
    VRMessageProcessor(number - 1, replicaId, broker)
    broker.start()
}
