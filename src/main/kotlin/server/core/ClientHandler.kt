package server.core

import common.Command
import common.Connection
import common.exception.ProtocolParseException
import common.resp.Frame
import common.resp.FrameParser
import org.slf4j.LoggerFactory
import java.net.SocketException

class ClientHandler(private val database: Database, private val connection: Connection) : Runnable {
    private val log = LoggerFactory.getLogger(ClientHandler::class.java)
    override fun run() {
        while (true) {
            try {
                val frame = connection.readFrame()
                var parser = FrameParser(frame)
                val command = Command.toCommand(parser)
                if (command is Command.Unknown) {
                    connection.writeFrame(Frame.FError("Unknown command: $command"))
                } else {
                    val result = database.executeCmd(command)
                    connection.writeFrame(result)
                }
            } catch (e: ProtocolParseException) {
                connection.writeFrame(Frame.FError(e.message!!))
            } catch (e: SocketException) {
                log.error("Connection closed", e)
                break
            }
        }
    }
}