package server.writerReader

import server.bloom.Bloom
import server.core.DBRecord
import server.core.KVMetadata
import server.enums.DataType
import server.storage.BlockHolder
import server.storage.SegmentMetadata
import java.io.File
import java.io.RandomAccessFile
import java.util.TreeMap

open class IndexReader(private val f: File) : Iterable<DBRecord?> {
    private val reader: RandomAccessFile = RandomAccessFile(f.path, "r")
    var metadata: SegmentMetadata? = null
        get() = field
        private set

    private var lastString = byteArrayOf()

    fun readMetadata(): SegmentMetadata {
        val currentPosition = reader.filePointer
        val level = readLevel()
        val fileId = readVInt()

        // jump to the footer
        seek(f.length() - 3 * Long.SIZE_BYTES)
        val keyRangeOffset = readLong()
        val blocksIndexOffset = readLong()
        val filterOffset = readLong()

        seek(keyRangeOffset)
        val firstKey = readString()
        val lastKey = readString()

        val blocksIndex = readBlocksIndex(blocksIndexOffset, filterOffset)

        // read filter
        val bloom = readFilter()

        seek(currentPosition)

        return SegmentMetadata(
            level,
            fileId,
            blocksIndex,
            keyRangeOffset,
            bloom,
            firstKey,
            lastKey
        )
    }

    private fun readBlocksIndex(start: Long, end: Long): TreeMap<String, BlockHolder> {
        seek(start)
        val blocksIndex = TreeMap<String, BlockHolder>()
        while (reader.filePointer <= end) {
            blocksIndex.put(readString(), BlockHolder(readVLong()))
        }
        return blocksIndex
    }

    private fun readFilter(): Bloom {
        val seed = readVInt()
        val k = readVInt()
        val bitMapSize = readVInt()
        val bitMap = LongArray(bitMapSize)
        for (i in 0 until bitMapSize) {
            bitMap[i] = readVLong()
        }
        return Bloom.restore(bitMap, seed.toLong(), k)
    }

    override fun iterator(): Iterator<DBRecord?> {
        return DBRecordIterator()
    }

    // it does what you think
    inner class DBRecordIterator : Iterator<DBRecord?> {
        private var current: DBRecord?
        val metadata: SegmentMetadata

        init {
            if (this@IndexReader.metadata == null) {
                this@IndexReader.metadata = readMetadata()
            }
            this.metadata = this@IndexReader.metadata!!
            current = getNextRecord()
        }

        override fun hasNext(): Boolean {
            return current != null
        }

        override fun next(): DBRecord? {
            val r = current
            current = if (reader.filePointer >= metadata.blocksEndOffset) {
                null
            } else {
                val operation = getNextRecord()
                operation
            }
            return r
        }

        fun current(): DBRecord? {
            return current
        }
    }

    open fun getNextRecord(): DBRecord? {
        val meta = readKVmeta()

        if (meta == null) return null

        val key = readString()

        val v: Any

        if (meta.vType == DataType.INT) {
            v = readVInt()
            if (v == -1) return null
        } else {
            v = readBytes()
        }

        return DBRecord(meta.op, key, v)
    }

    fun seek(pos: Long) {
        reader.seek(pos)
    }

    fun getFilePointer(): Long {
        return reader.filePointer
    }

    fun close() {
        reader.close()
    }

    fun closeAndRemove() {
        close()
        f.delete()
    }

    fun readKeyIgnoringKvMeta(): String {
        reader.seek(reader.filePointer + 1)
        return readString()
    }
    private fun readLevel(): Int {
        return reader.read()
    }

    private fun readInt(): Int {
        var v = 0
        for (i in 0 until Int.SIZE_BYTES) {
            v = (reader.read() shl (Byte.SIZE_BYTES * i)) or v
        }
        return v
    }

    private fun readLong(): Long {
        var v = 0L
        for (i in 0 until Long.SIZE_BYTES) {
            v = (reader.read().toLong() shl (Byte.SIZE_BYTES * i)) or v
        }
        return v
    }

    private fun readVInt(): Int {
        return readVLong().toInt()
    }

    private fun readVLong(): Long {
        var readN = reader.read().toLong()
        // the highest bit indicates if there are more bytes to be read
        // so, we only need the last 7 bits
        // 0x7f == 1111111
        var v = readN and 0x7f

        var shift = 7
        // 0x80 == 10000000
        while ((readN and 0x80) != 0L) {
            readN = reader.read().toLong()
            v = v or ((readN and 0x7F) shl shift)
            shift += 7
        }

        return v
    }

    private fun readBytes(): ByteArray {
        val vLen = readVInt()
        val vBytes = ByteArray(vLen)
        reader.read(vBytes)
        return vBytes
    }

    private fun readPrefixSharedString(): String {
        val prefixSize = readVInt()
        val kLen = readVInt()
        val kBytes = ByteArray(kLen)
        reader.read(kBytes)
        val currentBytes = lastString.sliceArray(0 until prefixSize) + kBytes
        val key = String(currentBytes)
        lastString = currentBytes
        return key
    }

    private fun readString(): String {
        val kLen = readVInt()
        val kBytes = ByteArray(kLen)
        reader.read(kBytes)
        return String(kBytes, Charsets.UTF_8)
    }

    private fun readKVmeta(): KVMetadata? {
        val meta = reader.read()
        if (meta == -1) return null
        return KVMetadata(meta)
    }
}
