import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.net.Socket
import java.nio.charset.StandardCharsets

fun main() {
    val s = Socket("localhost", 8000)
    val output = s.getOutputStream()
    val input = BufferedReader(InputStreamReader(s.getInputStream()))
    while (true){
        print(">> ")
        val line = readLine()
        line?.toByteArray(StandardCharsets.UTF_8)?.let { output.write(it) }
        println(input.readLine())
    }
}