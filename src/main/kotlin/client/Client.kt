package client

import common.Command
import common.Connection
import common.resp.Frame
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.net.Socket
import java.util.concurrent.CountDownLatch

class Client(host: String = "localhost", port: Int = 8000) {
    private val connection = Connection(Socket(host, port))

    fun set(key: String, value: Int) {
        set(key, byteArrayOf(value.toByte()))
    }

    fun set(key: String, value: String) {
        set(key, value.toByteArray(Charsets.UTF_8))
    }

    fun set(key: String, value: ByteArray) {
        val response = connection.writeCommand(Command.Set(key, value))
        if (response !is Frame.FString || response.data != "OK") {
            throw RuntimeException("unexpected frame $response")
        }
    }
}

class TestClient(val n: Int){
    fun start(){
        val cd = CountDownLatch(n)
        for (i in 0 until n){
            Thread{
                val s = Socket("localhost", 8000)
                val output = ObjectOutputStream(s.getOutputStream())
                val input = ObjectInputStream(s.getInputStream())
                output.writeObject("client_$i")
                for (j in 0..1000){
                    print(">> ")
                    val line = "set key_${i}_$j $j"
                    output.writeObject(line)
                    println(input.readObject())
                }
                cd.countDown()
            }.start()
        }
        cd.await()
    }
}
fun main() {
   Client().set("a", 123)
}