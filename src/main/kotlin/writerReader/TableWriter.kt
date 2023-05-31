package writerReader

import bloom.Bloom
import common.Config
import common.DBOperation
import segments.SegmentMetadata
import common.Utils
import java.io.RandomAccessFile
import java.io.Writer
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

    private var filter: Bloom? = null

    private var currentName = ""

    var currentPath = ""
        private set

    private var currentID = id.get()

    fun reserveSpaceForMetadata() {
        filter = Bloom(100, seed = currentID.toLong())
        val wt = writer!!
        for (i in 1..SegmentMetadata.nOfbytesForMetadata) {
            wt.write(0)
        }
        pointer = wt.filePointer
    }

    fun write(op: DBOperation) {
        val wt = writer!!
        lastKeyValueOffset = wt.filePointer
        filter?.add(op.k)
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
        currentName = "${basicPath}_${id.incrementAndGet()}"
        currentPath = "$prefix/$currentName"
        currentID = id.get()
        pointer = 0
        filter = null
        blockOffsets.clear()
        writer = RandomAccessFile(currentPath, "rws")
    }

    fun fillMetadata(level: Int = 0) {
        val wt = writer!!
        val footerStartOffset = wt.filePointer
        writeFooter()
        writeHeader(wt, level, footerStartOffset)
    }

    private fun writeHeader(wt: RandomAccessFile, level: Int, footerStartOffset: Long){
        wt.seek(0)
        wt.write(level)
        writeInt(footerStartOffset.toInt())
    }

    private fun writeInt(n: Int) {
        val wt = writer!!
        var v = n
        for (i in 1..4) {
            wt.write(v and 0xff)
            v = v shr 8
        }
    }


    private fun writeFooter() {
        writeBlocksOffset()
        writeFilter()
    }

    private fun writeBlocksOffset() {
        writeVint(blockOffsets.size)
        for (checkpoint in blockOffsets) {
            writeVint(checkpoint)
        }
    }

    private fun writeFilter() {
        val f = filter!!
        writeVint(f.seed.toInt())
        writeVint(f.k)
        writeVint(f.bitmap.size)
        for (l in f.bitmap) {
            writeVLong(l)
        }
    }
}
