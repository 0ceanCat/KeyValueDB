package server

import common.Connection
import common.Utils
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import server.core.ClientHandler
import server.core.Database
import server.storage.Merger
import server.writerReader.GeneralWriter
import server.writerReader.WALWriter
import java.net.ServerSocket

class Server(private val port: Int = 8000) {
    private val logger: Logger = LogManager.getLogger(Server::class)
    private val database = Database()

    private fun reloadFromWAL() {
        for (f in Utils.readFilesFrom(GeneralWriter.FOLDER) { it.startsWith(WALWriter.WAL_PREFIX) }) {
            database.recoverFromWal(f)
        }
    }

    fun runServer(){
        // init database

        // check the WAL file and reload the records to memory
        reloadFromWAL()

        // start the Merger thread
        Merger.start()

        val server = ServerSocket(port)
        logger.info("server.Server started...")
        logger.info("server.Server is listening on port ${port}")
        try {
            while (true) {
                val client = server.accept()
                logger.info("Accepted a client")
                Thread.startVirtualThread(ClientHandler(database, Connection(client)))
            }
        } finally {
            logger.info("shutdown...")
            database.close()
        }
    }
}


fun main() {
    Server().runServer()
}