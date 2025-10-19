import client.Client
import java.util.concurrent.CountDownLatch

class TestClient(val n: Int){
    fun start(){
        val cd = CountDownLatch(n)
        for (i in 0 until n){
            Thread{
                val client = Client()
                for (j in 0..1000){
                    client.set("$j", "$j")
                }
                cd.countDown()
            }.start()
        }
        cd.await()
    }
}

fun main() {
    TestClient(1).start()
}