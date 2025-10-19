package server.writerReader

import server.bloom.Bloom
import server.Config
import server.Config.Companion.BLOOM_FILTER_SIZE
import server.core.DBRecord
import common.Utils
import server.core.MemoryTable
import server.storage.SegmentMetadata
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

    private val blocksOffset = mutableListOf<Pair<String, Long>>()

    private var sharePrefix = false

    private var filter: Bloom? = null

    private var currentName = ""

    var currentPath = ""
        private set

    private var currentID = id.get()

    private var firstKeyOfCurrentBlock: String? = null

    private var firstKeyOfSegment: String? = null

    private var lastKeyOfSegment: String? = null

    init {
        currentName = "${basicPath}_${id.incrementAndGet()}"
        currentPath = "$prefix/$currentName"
        pointer = 0
        filter = null
        blocksOffset.clear()
        writer = RandomAccessFile(currentPath, "rws")
    }

    fun writeTable(table: MemoryTable): String {
        println("write data to ${currentPath}...")
        writeHeader()
        for (entry in table) {
            write(entry.value)
        }
        writeBlockMetadataAndFooter()
        println("${currentPath} done")
        return currentPath
    }

    // write a record to disk
    private fun write(op: DBRecord) {
        val wt = writer!!
        filter?.add(op.key) // insert it to the bloom filter
        super.write(op, sharePrefix)
        // start sharing prefix
        sharePrefix = true

        if (firstKeyOfCurrentBlock == null) {
            firstKeyOfCurrentBlock = op.key
        }

        if (firstKeyOfSegment == null) {
            firstKeyOfSegment = op.key
        }

        lastKeyOfSegment = op.key

        // the current block is full, need to create a new block
        if (wt.filePointer - pointer >= Config.BLOCK_SIZE) {
            // store the start offset of the last block
            blocksOffset += Pair(firstKeyOfCurrentBlock!!, pointer)
            // update the pointer
            pointer = wt.filePointer
            // stop sharing prefix with the previous block
            sharePrefix = false
            firstKeyOfCurrentBlock = null
        }
    }

    private fun writeHeader(level: Int = 0){
        val wt = writer!!
        wt.seek(0)
        wt.write(level)
        writeVint(currentID)
    }

    private fun writeBlockMetadataAndFooter() {
        val keyRangeOffset = writer!!.filePointer
        writeString(firstKeyOfSegment!!.toByteArray(Charsets.UTF_8))
        writeString(lastKeyOfSegment!!.toByteArray(Charsets.UTF_8))
        val blockIndexOffset = writer!!.filePointer
        writeBlocksIndex()
        val filterOffset = writer!!.filePointer
        writeFilter()
        writeLong(keyRangeOffset)
        writeLong(blockIndexOffset)
        writeLong(filterOffset)
    }

    private fun writeBlocksIndex() {
        // write the blocks offset
        for ((key, blockOffset) in blocksOffset) {
            val byteArray = key.toByteArray(Charsets.UTF_8)
            writeString(byteArray)
            writeVLong(blockOffset)
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

    private fun writeLong(n: Long) {
        val wt = writer!!
        var v = n
        for (i in 1..Long.SIZE_BYTES) {
            wt.write((v and 0xff).toInt())
            v = v shr Byte.SIZE_BYTES
        }
    }
}
