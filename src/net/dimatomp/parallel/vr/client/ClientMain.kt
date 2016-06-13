package net.dimatomp.parallel.vr.client

import net.dimatomp.parallel.vr.broker.UDPMessageBroker
import java.util.*

/**
 * Created by dimatomp on 13.06.16.
 */
fun main(args: Array<String>) {
    val properties = Properties()
    val stream = ClassLoader.getSystemClassLoader().getResourceAsStream("dkvs.properties")
    properties.load(stream)
    stream.close()
    val broker = UDPMessageBroker(null, properties)
    val processor = ClientMessageProcessor(broker, if (args.isEmpty()) 0 else args[0].toLong())
    val brokerThread = Thread { broker.start() }
    brokerThread.start()
    try {
        while (true)
            processor.parseAndSend(readLine()!!)
    } catch (e: Throwable) {
        brokerThread.interrupt()
        throw e
    }
}
