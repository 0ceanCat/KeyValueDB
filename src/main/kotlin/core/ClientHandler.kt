package core

import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.net.Socket

class ClientHandler(private val database: Database, private val client: Socket) : Runnable {
    private val clientName: String
    private val input = ObjectInputStream(client.getInputStream())
    private val output = ObjectOutputStream(client.getOutputStream())

    init {
        clientName = input.readObject() as String
        println("Accepted a client: $clientName")
    }

    override fun run() {
        try {
            while (true) {
                val readObject = (input.readObject() as String).trim()
                val strings = readObject.split(" ")
                var response: String? = null
                when (strings[0]) {
                    "set" -> {
                        try {
                            val key = strings[1].trim()
                            if (strings.size > 2) {
                                val vString = strings.subList(2, strings.size).joinToString(" ")
                                val v = vString.toIntOrNull() ?: vString
                                database.insert(key, v)
                                response = "success"
                                println("$clientName: Set $key to $v")
                            } else {
                                response = "rip"
                            }
                        } catch (e: Exception) {
                            response = "failed"
                        }
                    }

                    "get" -> {
                        if (strings.size != 2) {
                            response = "ah?"
                        } else {
                            val key = strings[1].trim()
                            val value = database.get(key)

                            println("$clientName: get ${key}")
                            value?.let {
                                response = value.toString()
                                value
                            } ?: let { response = "null" }

                        }
                    }

                    "del" -> {
                        response = if (strings.size != 2) {
                            "ah?"
                        } else {
                            val key = strings[1].trim()
                            database.delete(key)
                            println("$clientName: del ${key}")
                            "success"
                        }
                    }

                    else -> {
                        response = "ah?"
                    }
                }

                output.writeObject(response)
            }
        } catch (e: Exception) {
            e.printStackTrace()
        }finally {
                println("Client $clientName left...")
            }
        }
}