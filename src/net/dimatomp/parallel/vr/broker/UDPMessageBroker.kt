package net.dimatomp.parallel.vr.broker

import net.dimatomp.parallel.vr.api.MessageBroker
import net.dimatomp.parallel.vr.api.MessageProcessor
import net.dimatomp.parallel.vr.dkvs.Address
import net.dimatomp.parallel.vr.dkvs.Client
import net.dimatomp.parallel.vr.dkvs.Message
import net.dimatomp.parallel.vr.dkvs.Replica
import org.slf4j.LoggerFactory
import java.io.*
import java.net.*
import java.util.*
import java.util.concurrent.*
import kotlin.properties.Delegates

/**
 * Created by dimatomp on 12.06.16.
 */
class UDPMessageBroker(val nodeNumber: Int?, private val properties: Properties): MessageBroker<Message, Address> {
    companion object {
        private val logger = LoggerFactory.getLogger(UDPMessageBroker::class.java)
    }

    private val suspended: MutableList<DatagramPacket> = ArrayList()
    private var datagramSocket: DatagramSocket? = null
    override val myAddress: Address
        get() = if (nodeNumber == null) Client(datagramSocket!!.localSocketAddress) else Replica(nodeNumber)

    private fun socketAddress(address: String): SocketAddress {
        val url = URL("http://" + address)
        return InetSocketAddress(url.host, url.port)
    }

    private fun replicaAddress(number: Int): SocketAddress = socketAddress(properties.getProperty("node.$number"))

    private val buf = ByteArray(1 shl 20)
    private val packet = DatagramPacket(buf, buf.size)
    private val bufInput = ByteArrayInputStream(buf)

    fun start() {
        try {
            if (nodeNumber != null)
                datagramSocket = DatagramSocket(replicaAddress(nodeNumber))
            else
                datagramSocket = DatagramSocket(0)
            for (packet in suspended)
                datagramSocket!!.send(packet)
            suspended.clear()
            while (true) {
                datagramSocket!!.receive(packet)
                val m = try {
                    bufInput.reset()
                    // TODO Try to recycle ObjectInputStreams
                    ObjectInputStream(bufInput).readObject() as Message
                } catch (e: IOException) {
                    logger.error("Failed to read a message from ${packet.address}", e)
                    continue
                }
                logger.debug("Received message $m")
                synchronized(this) { client.onMessage(m) }
            }
        } finally {
            cancelScheduled()
            datagramSocket?.close()
            datagramSocket = null
        }
    }

    private var client: MessageProcessor<Message, Address> by Delegates.notNull<MessageProcessor<Message, Address>>()

    override fun registerClient(client: MessageProcessor<Message, Address>) {
        this.client = client
    }


    override fun sendMessage(m: Message, recipient: Address) {
        val byteArray = ByteArrayOutputStream()
        val stream = ObjectOutputStream(byteArray)
        stream.writeObject(m)
        stream.close()
        try {
            val recAddress: SocketAddress = when (recipient) {
                is Replica -> replicaAddress(recipient.number)
                is Client -> recipient.address as SocketAddress
                else -> throw IllegalArgumentException("Unsupported recipient type: $recipient")
            }
            val packet = DatagramPacket(byteArray.toByteArray(), byteArray.size(), recAddress)
            datagramSocket?.send(packet) ?: suspended.add(packet)
            logger.debug("Sent message $m to recipient $recipient")
        } catch (e: IOException) {
            logger.error("Failed to send a message to recipient $recipient", e)
        }
    }

    private val longInterval: Long = properties.getProperty("timeout").toLong()
    private val repeatedJobExecutor = Executors.newSingleThreadScheduledExecutor()
    private var job: ScheduledFuture<*>? = null

    override fun scheduleRepeated(interval: MessageBroker.Interval, operation: () -> Unit) {
        cancelScheduled()
        val time = if (interval == MessageBroker.Interval.SHORT)
            longInterval / 4
        else
            longInterval * (8 + (nodeNumber ?: 0)) / 10
        job = repeatedJobExecutor.scheduleWithFixedDelay(
                {
                    try {
                        synchronized(this) {
                            if (!Thread.interrupted())
                                operation()
                        }
                    } catch (e: Throwable) {
                        logger.error("Internal message processor error", e)
                    }
                }, time, time, TimeUnit.MILLISECONDS)
    }

    override fun cancelScheduled() {
        job?.cancel(true)
        job = null
    }
}