package net.dimatomp.parallel.vr.dkvs

import java.io.Serializable
import java.net.SocketAddress

/**
 * Created by dimatomp on 22.05.16.
 */
interface Message: Serializable
data class Request(val op: Operation, val client: Client, val requestNum: Long): Message
data class Prepare(val view: Int, val clientMessage: Request, val opNumber: Long, val commitNumber: Long): Message
data class PrepareOk(val view: Int, val opNumber: Long, val sender: Int): Message
data class Commit(val view: Int, val commitNumber: Long): Message
data class StartViewChange(val view: Int, val initiator: Int): Message
data class DoViewChange(val view: Int, val log: Log, val viewBeforeChange: Int, val opNumber: Long, val commitNumber: Long): Message

data class Log(val userReqs: MutableList<Request>): Serializable

interface Operation: Serializable
data class Get(val key: String): Operation
data class Set(val key: String, val value: String): Operation
data class Delete(val key: String): Operation
class Ping(): Operation

interface Response: Message
data class Value(val key: String, val value: String): Response
class NotFound(): Response
class Stored(): Response
class Deleted(): Response
class Pong(): Response

interface Address: Serializable
data class Replica(val number: Int): Address
data class Client(val address: SocketAddress): Address

interface Status
object ViewChange: Status
object Recovery: Status
