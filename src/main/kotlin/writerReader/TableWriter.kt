package writerReader

import common.DBOperation
import common.Utils
import java.io.File
import java.io.RandomAccessFile
import java.util.concurrent.atomic.AtomicInteger

class TableWriter : GeneralWriter() {
    companion object {
        private val prefix = "index"
        private val id = AtomicInteger(0)

        init {
            var max = -1
            for (f in Utils.readFilesFrom(prefix) {it.startsWith("segment")}) {
                max = maxOf(max, f.name.split("_")[1].toInt())
            }
            id.set(max + 1)
        }
    }

    private val basicPath = "segment"

    var currentPath = ""
        get() = field

    fun reserveSpaceForMetadata() {
        for (i in 1..5) {
            writer!!.write(0)
        }
    }

    override fun reset() {
        if (writer != null) close()
        startWrite()
    }

    private fun startWrite() {
        currentPath = "${basicPath}_${id.incrementAndGet()}"
        writer = RandomAccessFile("$prefix/$currentPath", "rws")
    }

    fun fillMetadata(level: Int = 0) {
        writer!!.seek(0)
        writer!!.write(level)
        var offset = lastOffset
        for (i in 1..4) {
            writer!!.write((offset and 0xff).toInt())
            offset = offset shr 8
        }
    }
}
