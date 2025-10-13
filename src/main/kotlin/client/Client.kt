package client

import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.net.Socket
import java.util.UUID
import java.util.concurrent.CountDownLatch

class Client(host: String = "localhost", port: Int = 8000) {
    private val id: String = UUID.randomUUID().toString()
    private val socket: Socket = Socket(host, port)
    private val input: BufferedInputStream = BufferedInputStream(socket.getInputStream())
    private val output: BufferedOutputStream = BufferedOutputStream(socket.getOutputStream())

    init {
        output.write(id.toByteArray(Charsets.UTF_8))
    }

    fun set(key: String, value: Int) {
        set(key, byteArrayOf(value.toByte()))
    }

    fun set(key: String, value: String) {
        set(key, value.toByteArray(Charsets.UTF_8))
    }

    private fun set(key: String, value: ByteArray) {
        output.write("$key $value")
        println(input.readObject())
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