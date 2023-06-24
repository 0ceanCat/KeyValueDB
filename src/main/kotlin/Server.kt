import common.DBRecord
import common.Database
import segments.Merger
import common.Utils
import enums.OperationType
import reactor.Reactor
import writerReader.GeneralWriter
import writerReader.IndexReader
import writerReader.WAL

class Server(private val port: Int = 8000) {
    private val database = Database()

    private fun checkWAL(): List<DBRecord> {
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
        val reloadedOperations = checkWAL()
        for (op in reloadedOperations) {
            if (op.op == OperationType.INSERT)
                database.insert(op.k, op.v)
            else
                database.delete(op.k)
        }

        // start the Merger thread
        Merger.start()

        Reactor(port, database).start()
    }
}


fun main() {
    Server().runServer()
}