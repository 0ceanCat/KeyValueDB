import client.Client
import java.util.concurrent.CountDownLatch

class TestClient(val n: Int){
    fun start(){
        val cd = CountDownLatch(n)
        for (i in 0 until n){
            Thread{
                val client = Client()
                for (j in 1..450){
                    client.set("$j", "${j*3}")
                }
                cd.countDown()
            }.start()
        }
        cd.await()
    }
}

fun main() {
    val client = Client()
    client.set("a", 2)
    client.set("b", "haha")
    println(client.get("a"))
    println(client.get("b"))
}