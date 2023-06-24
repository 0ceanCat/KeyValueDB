package reactor

import common.Database
import java.io.IOException
import java.net.InetSocketAddress
import java.nio.channels.SelectionKey
import java.nio.channels.Selector
import java.nio.channels.ServerSocketChannel

class Reactor(port: Int, private val database: Database) : Thread() {
    private val selector: Selector
    private val serverSocket: ServerSocketChannel

    init {
        selector = Selector.open()
        serverSocket = ServerSocketChannel.open()
        serverSocket.socket().bind(InetSocketAddress(port))
        serverSocket.configureBlocking(false)
        val sk = serverSocket.register(selector, SelectionKey.OP_ACCEPT)
        sk.attach(Acceptor())
        println("Listening on port $port")
    }

    override fun run() {
        try {
            while (!interrupted()) {
                selector.select()
                val selected = selector.selectedKeys()
                for (skTmp in selected) {
                    dispatch(skTmp)
                }
                selected.clear()
            }
        } catch (ex: IOException) {
            ex.printStackTrace()
        }
    }

    fun dispatch(k: SelectionKey) {
        val r = k.attachment() as Runnable
        r.run()
    }

    internal inner class Acceptor : Runnable {
        override fun run() {
            try {
                val sc = serverSocket.accept() // this accept() is no-blocking
                if (sc != null) {
                    println("Accepted a new Client")
                    Handler(selector, sc, database)
                }
            } catch (ex: IOException) {
                ex.printStackTrace()
            }
        }
    }
}
