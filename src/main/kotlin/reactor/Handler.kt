package reactor

import common.Database
import java.io.EOFException
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.SelectionKey
import java.nio.channels.Selector
import java.nio.channels.SocketChannel
import java.nio.charset.StandardCharsets
import java.util.concurrent.Executor
import java.util.concurrent.Executors

class Handler(sel: Selector, private val socket: SocketChannel, private val database: Database) : Runnable {
    private val sk: SelectionKey
    private val input = ByteBuffer.allocate(MAX_IN)
    private val output = ByteBuffer.allocate(MAX_OUT)
    private var state = READING

    companion object {
        private const val MAX_IN = 1024
        private const val MAX_OUT = 1024
        private const val READING = 0
        private const val SENDING = 1
        private const val CLOSED = 2
        private val workPool: Executor = Executors.newFixedThreadPool(1)
        private const val PROCESSING = 4
    }

    override fun run() {
        try {
            if (state == READING) read() else if (state == SENDING) send()
        } catch (ex: IOException) {
            try {
                sk.channel().close()
            } catch (ignore: IOException) {
            }
        }
    }

    private val request = StringBuilder()

    init {
        socket.configureBlocking(false)
        sk = socket.register(sel, 0)
        sk.interestOps(SelectionKey.OP_READ)
        sk.attach(this)
        sel.wakeup()
    }

    private fun inputIsComplete(bytes: Int): Boolean {
        if (bytes > 0) {
            input.flip()
            while (input.hasRemaining()) {
                val ch = input.get()
                if (ch.toInt() == 3) { // ctrl+c
                    state = CLOSED
                    return true
                } else {
                    request.append(ch.toInt().toChar())
                }
            }
            state = SENDING
            return true
        } else if (bytes == -1) {
            throw EOFException()
        }
        return false
    }

    private fun process() {
        when (state) {
            CLOSED -> {
                throw EOFException()
            }
            PROCESSING -> {
                val readObject = request.toString().trim()
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
                                println("Set $key to $v")
                            } else {
                                response = "rip"
                            }
                        } catch (e: Exception) {
                            response =  "failed"
                        }
                    }

                    "get" -> {
                        if (strings.size != 2) {
                            response = "ah?"
                        } else {
                            val key = strings[1].trim()
                            val value = database.get(key)

                            println("get ${key}")
                            value?.let {
                                response = value.toString()
                                value
                            } ?: let { response = "null" }

                        }
                    }

                    "del" -> {
                        if (strings.size != 2) {
                            response = "ah?"
                        } else {
                            val key = strings[1].trim()
                            database.delete(key)
                            println("del ${key}")
                            response = "success"
                        }
                    }

                    else -> {
                        response = "ah?"
                    }
                }
                output.put(response?.toByteArray(StandardCharsets.UTF_8))
                send()
            }
        }
    }

    private fun send() {
        var written = -1
        output.flip()
        if (output.hasRemaining()) {
            written = socket.write(output)
        }
        if (outputIsComplete(written)) {
            throw EOFException()
        } else {
            state = READING
            sk.interestOps(SelectionKey.OP_READ)
        }
    }

    private fun outputIsComplete(written: Int): Boolean {
        if (written <= 0) {
            return true
        }
        output.clear()
        request.delete(0, request.length)
        return false
    }

    private fun read() {
        input.clear()
        val n = socket.read(input)
        if (inputIsComplete(n)) {
            state = PROCESSING
            workPool.execute(Processor())
        }
    }

    private fun processAndHandOff() {
        try {
            process()
        } catch (e: EOFException) {
            try {
                sk.channel().close()
                println("A Client Left")
            } catch (ignored: IOException) {
            }
        }
    }

    private inner class Processor : Runnable {
        override fun run() {
            processAndHandOff()
        }
    }
}
