import common.DBRecord
import core.Database
import segments.Merger
import common.Utils
import core.ClientHandler
import enums.OperationType
import writerReader.GeneralWriter
import writerReader.IndexReader
import writerReader.WAL
import java.net.ServerSocket

class Server(private val port: Int = 8000) {
    private val database = Database()

    private fun reloadFromWAL(): List<DBRecord> {
        val operations = mutableListOf<DBRecord>()
        for (f in Utils.readFilesFrom(GeneralWriter.prefix) { it.startsWith(WAL.logFile) }) {
            println("realoding wal from ${f.name}...")
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
        println("Server started...")
        println("Server is listening on port ${port}")
        try {
            while (true) {
                val client = server.accept()
                Thread.startVirtualThread(ClientHandler(database, client))
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