package writerReader

import common.Utils
import java.io.RandomAccessFile
import java.util.concurrent.atomic.AtomicInteger

class TableWriter : GeneralWriter() {
    companion object {
        private val prefix = "index"
        private val id = AtomicInteger(0)

        init {
            var max = -1
            for (f in Utils.readFilesFrom(prefix) { it.startsWith("segment") }) {
                max = maxOf(max, f.name.split("_")[1].toInt())
            }
            id.set(max + 1)
        }
    }

    private val basicPath = "segment"

    var currentPath = ""
        get() = field

    fun reserveSpaceForMetadata(nBytes: Int) {
        for (i in 1..nBytes) {
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

    fun fillMetadata(level: Int = 0, checkpoints: List<Int> = mutableListOf()) {
        val wt = writer!!
        wt.seek(0)
        wt.write(level)
        wt.write(checkpoints.size)
        for (checkpoint in checkpoints) {
            var cp = checkpoint
            for (i in 1..4) {
                wt.write(cp and 0xff)
                cp = cp shr 8
            }
        }
    }
}
