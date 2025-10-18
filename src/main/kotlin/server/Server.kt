package server

import common.Connection
import common.Utils
import server.core.DBRecord
import server.core.ClientHandler
import server.core.Database
import server.enums.OperationType
import server.segments.Merger
import server.writerReader.GeneralWriter
import server.writerReader.IndexReader
import server.writerReader.WAL
import java.net.ServerSocket

class Server(private val port: Int = 8000) {
    private val database = Database()

    private fun reloadFromWAL(): List<DBRecord> {
        val operations = mutableListOf<DBRecord>()
        for (f in Utils.readFilesFrom(GeneralWriter.prefix) { it.startsWith(WAL.logFile) }) {
            println("reloading wal from ${f.name}...")
            val reader = IndexReader(f)
            var dbOperation = reader.getNextRecord()
            while (dbOperation != null) {
                operations += dbOperation
                dbOperation = reader.getNextRecord()
            }
            reader.closeAndRemove()
        }
        return operations
    }

    fun runServer(){
        // init database

        // check the WAL file and reload the records to memory
        val reloadedOperations = reloadFromWAL()
        for (op in reloadedOperations) {
            if (op.op == OperationType.INSERT)
                database.insert(op.k, op.v)
            else
                database.delete(op.k)
        }

        // start the Merger thread
        Merger.start()

        val server = ServerSocket(port)
        println("server.Server started...")
        println("server.Server is listening on port ${port}")
        try {
            while (true) {
                val client = server.accept()
                println("Accepted a client")
                Thread.startVirtualThread(ClientHandler(database, Connection(client)))
            }
        } finally {
            println("shutdown...")
            database.close()
        }
    }
}


fun main() {
    Server().runServer()
}