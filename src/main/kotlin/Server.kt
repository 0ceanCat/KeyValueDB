import common.DBRecord
import common.Database
import segments.Merger
import common.Utils
import enums.OperationType
import writerReader.GeneralWriter
import writerReader.IndexReader
import writerReader.WAL
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.net.ServerSocket
import java.net.Socket

class Server(val port: Int = 8000) {
    private val database: Database

    init {
        // init database

        // check the WAL file and reload the records to memory
        val reloadedOperations = checkWAL()
        database = Database()
        for (op in reloadedOperations) {
            if (op.op == OperationType.INSERT)
                database.insert(op.k, op.v)
            else
                database.delete(op.k)
        }

        // start the Merger thread
        Merger.start()
    }

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

    inner class Worker(private val client: Socket) : Thread() {
        override fun run() {
            val input = ObjectInputStream(client.getInputStream())
            val output = ObjectOutputStream(client.getOutputStream())
            try {
                while (true) {
                    val readObject = (input.readObject() as String).trim()
                    val strings = readObject.split(" ")
                    when (strings[0]) {
                        "set" -> {
                            try {
                                val key = strings[1].trim()
                                if (strings.size > 2) {
                                    val vString = strings.subList(2, strings.size).joinToString(" ")
                                    val v = vString.toIntOrNull() ?: vString
                                    database.insert(key, v)
                                    output.writeObject("success")
                                    println("Set $key to $v")
                                } else {
                                    output.writeObject("rip")
                                }
                            } catch (e: Exception) {
                                output.writeObject("failed")
                            }
                        }

                        "get" -> {
                            if (strings.size != 2) {
                                output.writeObject("ah?")
                            } else {
                                val key = strings[1].trim()
                                val value = database.get(key)
                                value?.let {
                                    output.writeObject(value)
                                    value
                                } ?: let { output.writeObject("null") }

                            }
                        }

                        "del" -> {
                            if (strings.size != 2) {
                                output.writeObject("ah?")
                            } else {
                                val key = strings[1].trim()
                                database.delete(key)
                                output.writeObject("success")
                            }
                        }

                        else -> {
                            output.writeObject("ah?")
                        }
                    }
                }
            } finally {
                println("A client left...")
            }

        }
    }

    fun runServer() {
        val server = ServerSocket(port)
        println("Server started...")
        try {
            while (true) {
                Worker(server.accept()).start()
                println("Accepted a client...")
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