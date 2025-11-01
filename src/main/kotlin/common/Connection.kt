package common

import common.resp.Frame
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.net.Socket

class Connection(socket: Socket) {
    private val input: BufferedInputStream = BufferedInputStream(socket.getInputStream())
    private val output: BufferedOutputStream = BufferedOutputStream(socket.getOutputStream())

    fun writeCommand(cmd: Command): Frame {
        output.writeArrayFrame(cmd.toFrame())
        return readFrame()
    }

    fun writeFrame(frame: Frame) {
        output.write(frame.encode())
        output.flush()
    }

    fun readFrame(): Frame {
        return Frame.decodedFrom(input)
    }
}