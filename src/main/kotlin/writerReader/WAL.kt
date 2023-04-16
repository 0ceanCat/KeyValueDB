package writerReader

import common.Utils
import java.io.File
import java.io.RandomAccessFile

class WAL : GeneralWriter() {
    companion object {
        private var id = -1
        val logFile = "binlog"
    }

    private var current_path = ""
    init {
        loadId()
        current_path = "${prefix}/${logFile}_$id"
        writer = RandomAccessFile(current_path, "rws")
        id++
    }

    private fun loadId(){
        var max = -1
        for (f in Utils.readFilesFrom(prefix) {it.startsWith(logFile)}) {
            max = maxOf(max, f.name.split("_")[1].toInt())
        }
        id = max + 1
    }
    override fun reset() {
        if (writer != null) close()
    }

    override fun close() {
        super.close()
        File(current_path).delete()
    }

}