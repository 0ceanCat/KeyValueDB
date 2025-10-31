package server.writerReader

import common.Utils
import server.Config
import server.Config.Companion.BLOOM_FILTER_SIZE
import server.bloom.Bloom
import server.core.DBRecord
import server.core.MemoryTable
import server.enums.DataType
import server.enums.OperationType
import java.io.Closeable
import java.io.File
import java.io.FileOutputStream
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger
import java.util.logging.Logger

abstract class GeneralWriter(protected val fos: FileOutputStream) : Closeable {
    companion object {
        const val FOLDER = "index"
    }

    private var lastString: ByteArray = byteArrayOf()
    protected val fc: FileChannel = fos.channel

    init {
        if (!Files.exists(Path.of(FOLDER))) {
            Files.createDirectory(Path.of(FOLDER))
        }
    }

    // write a record to disk
    abstract fun write(op: DBRecord)

    protected fun writeVint(_v: Int) {
        writeVLong(_v.toLong())
    }

    protected fun writeVLong(_v: Long) {
        var v = _v
        while ((v and 0x7F.inv()) != 0L) {
            fos.write((v and 0x7F or 0x80).toInt())
            v = v ushr 7
        }
        fos.write(v.toInt())
    }

    protected fun writeKeySharingPrefix(key: String) {
        val bytes = key.toByteArray()
        var sharedPrefix = 0

        for (i in lastString.indices) {
            if (lastString[i] == bytes[i]) sharedPrefix++
            else break
        }

        lastString = bytes

        writeVint(sharedPrefix)
        writeBytes(bytes.sliceArray(sharedPrefix until bytes.size))
    }

    protected fun writeWithoutSharingPrefix(key: String) {
        val bytes = key.toByteArray()
        lastString = bytes
        writeVint(0)
        writeBytes(bytes)
    }

    protected fun writeBytes(bytes: ByteArray) {
        val len = bytes.size
        writeVint(len)
        fos.write(bytes)
    }

    override fun close() {
        fos.close()
    }

    protected fun writeKvMeta(
        op: OperationType,
        vType: DataType
    ) {
        fos.write(op.id + vType.id)
    }

    protected fun write(
        op: OperationType,
        k: String,
        v: ByteArray,
        sharePrefix: Boolean = true
    ) {
        writeKvMeta(op, DataType.STRING)
        if (sharePrefix)
            writeKeySharingPrefix(k)
        else
            writeWithoutSharingPrefix(k)
        writeBytes(v)
    }

    protected fun write(
        op: OperationType,
        k: String,
        v: Int,
        sharePrefix: Boolean = true
    ) {
        writeKvMeta(op, DataType.INT)
        if (sharePrefix)
            writeKeySharingPrefix(k)
        else
            writeWithoutSharingPrefix(k)
        writeVint(v)
    }
}

class TableWriter(val level: Int, fos: FileOutputStream = FileOutputStream("${BASIC_PATH}_${id.incrementAndGet()}")) : GeneralWriter(fos) {
    private val log: Logger = Logger.getLogger(TableWriter::class.java.name)

    companion object {
        private const val PREFIX = "index"
        private const val BASIC_PATH = "segment"
        private val id = AtomicInteger(0)

        init {
            var max = -1
            for (f in Utils.readFilesFrom(PREFIX) { it.startsWith("segment") }) {
                max = maxOf(max, f.name.split("_")[1].toInt())
            }
            id.set(max + 1)
        }
    }

    private var pointer = 0L

    private val blocksOffset = mutableListOf<Pair<String, Long>>()

    private var sharePrefix = false

    private val filter: Bloom

    private val currentName = "${BASIC_PATH}_${id.incrementAndGet()}"

    private var currentID = id.get()

    private var firstKeyOfCurrentBlock: String? = null

    private var firstKeyOfSegment: String? = null

    private var lastKeyOfSegment: String? = null

    val currentPath = "$PREFIX/$currentName"

    init {
        pointer = 0
        filter = Bloom(BLOOM_FILTER_SIZE, seed = currentID.toLong())
        blocksOffset.clear()
        writeHeader()
    }

    fun writeTable(table: MemoryTable): String {
        log.info("Write memtable to $currentPath...")
        for (entry in table) {
            write(entry.value)
        }
        log.info("Memtable all written to $currentPath")
        return currentPath
    }

    override fun write(op: DBRecord) {
        filter.add(op.key) // insert it to the bloom filter
        write(op, sharePrefix)
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
        if (fc.position() - pointer >= Config.BLOCK_SIZE) {
            // store the start offset of the last block
            blocksOffset += Pair(firstKeyOfCurrentBlock!!, pointer)
            // update the pointer
            pointer = fc.position()
            // stop sharing prefix with the previous block
            sharePrefix = false
            firstKeyOfCurrentBlock = null
        }
    }

    private fun write(record: DBRecord, sharePrefix: Boolean)  {
        val vType = if (record.value is Int) DataType.INT else DataType.STRING
        if (vType == DataType.STRING) {
            // the value is a string
            write(record.op, record.key, record.value as ByteArray, sharePrefix)
        } else {
            // the value is an int
            write(record.op, record.key, record.value as Int, sharePrefix)
        }
    }

    fun writeHeader(){
        fos.write(level)
        writeVint(currentID)
        pointer = fc.position()
    }

    private fun writeBlockMetadataAndFooter() {
        val keyRangeOffset = fc.position()
        writeBytes(firstKeyOfSegment!!.toByteArray(Charsets.UTF_8))
        writeBytes(lastKeyOfSegment!!.toByteArray(Charsets.UTF_8))
        val blockIndexOffset = fc.position()
        writeBlocksIndex()
        val filterOffset = fc.position()
        writeFilter()
        writeLong(keyRangeOffset)
        writeLong(blockIndexOffset)
        writeLong(filterOffset)
    }

    override fun close() {
        writeBlockMetadataAndFooter()
        super.close()
    }

    private fun writeBlocksIndex() {
        // write the blocks offset
        for ((key, blockOffset) in blocksOffset) {
            val byteArray = key.toByteArray(Charsets.UTF_8)
            writeBytes(byteArray)
            writeVLong(blockOffset)
        }
    }

    private fun writeFilter() {
        writeVint(filter.seed.toInt())
        writeVint(filter.k)
        writeVint(filter.bitmap.size)
        for (l in filter.bitmap) {
            writeVLong(l)
        }
    }

    private fun writeLong(n: Long) {
        var v = n
        repeat(Long.SIZE_BYTES) {
            fos.write((v and 0xff).toInt())
            v = v shr Byte.SIZE_BYTES
        }
    }
}

class WALWriter: GeneralWriter {
    companion object {
        private val id = loadId()
        const val WAL_PREFIX = "binlog"

        private fun loadId(): Int{
            var max = -1
            for (f in Utils.readFilesFrom(FOLDER) {it.startsWith(WAL_PREFIX)}) {
                max = maxOf(max, f.name.split("_")[1].toInt())
            }
            return max + 1
        }
    }

    constructor() : super(FileOutputStream("${FOLDER}/${WAL_PREFIX}_$id"))
    private var currentPath = "${FOLDER}/${WAL_PREFIX}_$id"

    override fun write(record: DBRecord) {
        val vType = if (record.value is Int) DataType.INT else DataType.STRING
        writeKvMeta(record.op, vType)
        writeBytes(record.key.toByteArray(Charsets.UTF_8))
        if (vType == DataType.STRING) {
            // the value is a string
            writeBytes(record.value as ByteArray)
        } else {
            // the value is an int
            writeVint(record.value as Int)
        }
    }

    override fun close() {
        super.close()
    }

    fun delete() {
        File(currentPath).delete()
    }
}