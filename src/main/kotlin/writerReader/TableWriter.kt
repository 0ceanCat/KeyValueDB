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

    private var sharePrefix = false

    var currentPath = ""
        get() = field

    fun reserveSpaceForMetadata() {
        val wt = writer!!
        for (i in 1..SegmentMetadata.nOfbytesForMetadata) {
            wt.write(0)
        }
        pointer = wt.filePointer
    }

    fun write(op: DBOperation) {
        val wt = writer!!
        lastKeyValueOffset = wt.filePointer
        super.write(op, sharePrefix)
        sharePrefix = true
        if (wt.filePointer - pointer >= Config.BLOCK_SIZE) {
            blockOffsets += pointer.toInt()
            pointer = wt.filePointer
            sharePrefix = false
        }
    }

    override fun reset() {
        writer?.close()
        startWrite()
    }

    private fun startWrite() {
        currentPath = "${basicPath}_${id.incrementAndGet()}"
        pointer = 0
        blockOffsets.clear()
        writer = RandomAccessFile("$prefix/$currentPath", "rws")
    }

    fun fillMetadata(level: Int = 0) {
        val wt = writer!!
        val currentPointer = wt.filePointer
        wt.seek(0)
        wt.write(level)
        writeInt(currentPointer.toInt())
        wt.seek(currentPointer)
        for (checkpoint in blockOffsets) {
            writeInt(checkpoint)
        }
    }

    private fun writeInt(n: Int) {
        val wt = writer!!
        var v = n
        for (i in 1..4) {
            wt.write(v and 0xff)
            v = v shr 8
        }
    }
}
