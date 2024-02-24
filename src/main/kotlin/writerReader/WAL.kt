package writerReader

import common.DBRecord
import common.Utils
import java.io.File
import java.io.RandomAccessFile

class WAL : GeneralWriter() {
    companion object {
        private var id = -1
        const val logFile = "binlog"
    }

    private var currentPath = ""

    override fun write(op: DBRecord, sharePrefix: Boolean) {
        if (writer == null){
            init()
        }
        super.write(op, sharePrefix)
    }

    override fun reset() {
        if (writer != null) close()
    }

    override fun close() {
        super.close()
        File(currentPath).delete()
    }
    private fun loadId(): Int{
        var max = -1
        for (f in Utils.readFilesFrom(prefix) {it.startsWith(logFile)}) {
            max = maxOf(max, f.name.split("_")[1].toInt())
        }
        return max + 1
    }

    private fun init(){
        id = loadId()
        currentPath = "${prefix}/${logFile}_$id"
        writer = RandomAccessFile(currentPath, "rws")
        id++
    }
}