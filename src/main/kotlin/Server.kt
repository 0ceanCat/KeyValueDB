import common.DBOperation
import common.Utils
import writerReader.GeneralWriter
import writerReader.IndexReader
import writerReader.WAL
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.net.ServerSocket
import java.net.Socket
import java.util.TreeMap

class Server(val port: Int = 8000) {
    private val threshold = 64
    private val sstable: MemoryTable
    private val searcher = Searcher()

    init {
        val reloadedOperations = checkWAL()
        sstable = MemoryTable(threshold)
        for (op in reloadedOperations)
            sstable.insert(op.k, op.v)
        Merger.start()
    }

    private fun checkWAL(): List<DBOperation> {
        val operations = mutableListOf<DBOperation>()
        for (f in Utils.readFilesFrom(GeneralWriter.prefix) { it.startsWith(WAL.logFile) }) {
            println("realoding wal from ${f.name}...")
            val reader = IndexReader(f)
            var dbOperation = reader.getNextOperation()
            while (dbOperation != null) {
                operations += dbOperation
                dbOperation = reader.getNextOperation()
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
                                val vString = strings.subList(2, strings.size).joinToString(" ")
                                val v = vString.toIntOrNull() ?: vString
                                sstable.insert(key, v)
                                output.writeObject("success")
                                println("Set $key to $v")
                            } catch (e: Exception) {
                                output.writeObject("failed")
                            }
                        }

                        "get" -> {
                            if (strings.size != 2) {
                                output.writeObject("ah?")
                            } else {
                                val key = strings[1].trim()
                                var value = sstable.get(key)
                                value = value ?: let { searcher.searchFromDisc(key) }
                                value?.let {
                                    output.writeObject(value)
                                    value
                                }?:let { output.writeObject("null") }

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
            sstable.close()
        }

    }
}


fun main() {
    Server().runServer()
}