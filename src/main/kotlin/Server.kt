import java.io.ObjectInputStream
import java.io.ObjectOutput
import java.io.ObjectOutputStream
import java.net.ServerSocket
import java.net.Socket
import java.util.StringJoiner

class Server(val port: Int = 8000) {
    val sstable = MemoryTable(16)

    inner class Worker(private val client: Socket) : Thread() {
        override fun run() {
            val input = ObjectInputStream(client.getInputStream())
            val output = ObjectOutputStream(client.getOutputStream())
            try {
                while (true) {
                    val readObject = input.readObject() as String
                    val strings = readObject.split(" ")
                    if (strings[0] == "set") {
                        try {
                            val key = strings[1]
                            val vString = strings.subList(2, strings.size).joinToString(" ")
                            val v = vString.toIntOrNull() ?: vString
                            sstable.insert(key, v)
                            output.writeObject("success")
                            println("Set $key to $v")
                        } catch (e: Exception) {
                            output.writeObject("failed")
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