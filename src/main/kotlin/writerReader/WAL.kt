package writerReader

import java.io.File
import java.io.RandomAccessFile

class WAL : GeneralWriter() {
    companion object {
        private var id = 0
    }

    private val path = "binlog"
    private var current_path = ""
    init {
        current_path = "${prefix}/${path}_$id"
        writer = RandomAccessFile(current_path, "rws")
        id++
    }

    override fun reset() {
        if (writer != null) close()
    }

    override fun close() {
        super.close()
        File(current_path).delete()
    }

}