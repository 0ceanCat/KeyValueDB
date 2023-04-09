import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.net.Socket

fun main() {
    val s = Socket("localhost", 8000)
    val output = ObjectOutputStream(s.getOutputStream())
    val input = ObjectInputStream(s.getInputStream())
    while (true){
        print(">> ")
        val line = readLine()
        output.writeObject(line)
        println(input.readObject())
    }
}