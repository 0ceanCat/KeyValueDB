package common

import common.resp.ARRAY_SYMBOL
import common.resp.CRLF
import common.resp.Frame
import java.io.OutputStream

fun OutputStream.writeArrayFrame(frame: Frame) {
    if (frame is Frame.FArray) {
        write(ARRAY_SYMBOL.toInt())
        writeInt(frame.array.size)
        for (bulk in frame.array) {
            write(bulk.encode())
        }
        flush()
    }
}


fun OutputStream.writeInt(value: Int) {
    write("$value".toByteArray(Charsets.UTF_8))
    write(CRLF)
}