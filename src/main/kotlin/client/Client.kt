package client

import common.Command
import common.Connection
import common.resp.Frame
import java.net.Socket

class Client(host: String = "localhost", port: Int = 8000) {
    private val connection = Connection(Socket(host, port))

    fun set(key: String, value: Int) {
        _set(key, value)
    }

    fun set(key: String, value: String) {
        _set(key, value.toByteArray(Charsets.UTF_8))
    }

    fun get(key: String): Any? {
        val response = connection.writeCommand(Command.Get(key))
        return when(response) {
            is Frame.FBulk -> String(response.data, Charsets.UTF_8)
            is Frame.FInteger -> response.data
            is Frame.FString -> response.data
            else -> {throw RuntimeException("unexpected frame $response")}
        }
    }

    private fun _set(key: String, value: Any) {
        val response = connection.writeCommand(Command.Set(key, value))
        if (response !is Frame.FString || response.data != "OK") {
            throw RuntimeException("unexpected frame $response")
        }
    }
}