import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.net.Socket
import java.util.concurrent.CountDownLatch

class Client(val clientName: String, val host: String = "localhost", val port: Int = 8000) {
    fun start() {
        val s = Socket(host, port)
        val output = ObjectOutputStream(s.getOutputStream())
        val input = ObjectInputStream(s.getInputStream())
        output.writeObject(clientName)
        while (true) {
            print(">> ")
            val line = readLine()
            output.writeObject(line)
            println(input.readObject())
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
    //TestClient(1).start()
   Client("A").start()
}