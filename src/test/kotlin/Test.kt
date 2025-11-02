import client.Client
import java.util.concurrent.CountDownLatch

class TestClient(val n: Int){
    fun start(){
        val cd = CountDownLatch(n)
        for (i in 0 until n){
            val client = Client()
            for (j in 1..10){
                client.set("$j", "${j*3}")
            }
            cd.countDown()
        }
        cd.await()
    }
}

fun main() {
    TestClient(1).start()
    val client = Client()
    println(client.get("1"))
    println(client.get("2"))
}