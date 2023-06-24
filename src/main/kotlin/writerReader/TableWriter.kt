package writerReader

import bloom.Bloom
import common.Config
import common.Config.Companion.BLOOM_FILTER_SIZE
import common.DBRecord
import segments.SegmentMetadata
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

    private val blocksOffset = mutableListOf<Int>()

    private var sharePrefix = false

    private var filter: Bloom? = null

    private var currentName = ""

    var currentPath = ""
        private set

    private var currentID = id.get()

    // reserve space for header
    fun reserveSpaceForHeader() {
        filter = Bloom(BLOOM_FILTER_SIZE, seed = currentID.toLong())
        val wt = writer!!
        for (i in 1..SegmentMetadata.nOfbytesForMetadata) {
            wt.write(0)
        }
        pointer = wt.filePointer
    }

    // write a record to disk
    fun write(op: DBRecord) {
        val wt = writer!!
        lastKeyValueOffset = wt.filePointer
        filter?.add(op.k) // insert it to the bloom filter
        super.write(op, sharePrefix)
        // start sharing prefix
        sharePrefix = true

        // the current block is full, need to create a new block
        if (wt.filePointer - pointer >= Config.BLOCK_SIZE) {
            // store the start offset of the last block
            blocksOffset += pointer.toInt()
            // update the pointer
            pointer = wt.filePointer
            // stop sharing prefix with the previous block
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
        blocksOffset.clear()
        writer = RandomAccessFile(currentPath, "rws")
    }

    // write segment's metadata to disc
    fun fillMetadata(level: Int = 0) {
        val wt = writer!!
        val footerStartOffset = wt.filePointer

        // write footer
        writeFooter()

        // write header
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
        writeVint(blocksOffset.size) // write the number of blocks

        // write the blocks offset
        for (checkpoint in blocksOffset) {
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
