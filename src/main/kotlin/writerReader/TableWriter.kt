package writerReader

import common.Config
import common.DBOperation
import common.SegmentMetadata
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

    private var pointer = 0L

    private var lastKeyValueOffset = 0L

    private val blockOffsets = mutableListOf<Int>()

    var currentPath = ""
        get() = field

    fun reserveSpaceForMetadata(nBytes: Int) {
        val wt = writer!!
        for (i in 1..nBytes) {
            wt.write(0)
        }
        pointer = wt.filePointer
    }

    override fun write(op: DBOperation) {
        val wt = writer!!
        lastKeyValueOffset = wt.filePointer
        super.write(op)
        if (wt.filePointer - pointer >= Config.BLOCK_SIZE) {
            blockOffsets += pointer.toInt()
            pointer = wt.filePointer
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
        blockOffsets += lastKeyValueOffset.toInt() // add the offset of the last key-value pair
        val wt = writer!!
        wt.seek(0)
        wt.write(level)
        for (checkpoint in blockOffsets) {
            var cp = checkpoint
            for (i in 1..4) {
                wt.write(cp and 0xff)
                cp = cp shr 8
            }
        }
    }
}
