package writerReader

import bloom.Bloom
import common.*
import enums.DataType
import segments.SegmentMetadata
import java.io.File
import java.io.RandomAccessFile

class IndexReader(private val f: File) : Iterable<DBOperation?> {
    private val reader: RandomAccessFile
    private val fileId: Int
    var metadata: SegmentMetadata? = null
        get() = field
        private set

    private var lastString = byteArrayOf()

    init {
        reader = RandomAccessFile(f.path, "r")
        fileId = f.name.split("_")[1].toInt()
    }

    fun readMetadata(): SegmentMetadata {
        val level = readLevel()
        val footerStartOffset = readInt()
        val currentOffset = getFilePointer()
        seek(footerStartOffset.toLong())

        // read offsets
        val numberOfBlocks = readVInt()
        val blocksOffset = mutableListOf<Int>()
        for (i in 1..numberOfBlocks)
            blocksOffset += readVInt()

        // read filter
        val seed = readVInt()
        val k = readVInt()
        val bitMapSize = readVInt()
        val bitMap = LongArray(bitMapSize)
        for (i in 0 until bitMapSize) {
            bitMap[i] = readVLong()
        }
        
        seek(currentOffset)
        return SegmentMetadata(
            level, footerStartOffset, fileId,
            blocksOffset, Bloom.restore(bitMap, seed.toLong(), k)
        )
    }

    override fun iterator(): Iterator<DBOperation?> {
        return DBOperationIterator()
    }

    inner class DBOperationIterator : Iterator<DBOperation?> {
        private var current: DBOperation?
        val metadata: SegmentMetadata

        init {
            if (this@IndexReader.metadata == null) {
                this@IndexReader.metadata = readMetadata()
            }
            this.metadata = this@IndexReader.metadata!!
            current = getNextOperation()
        }

        override fun hasNext(): Boolean {
            return current != null
        }

        override fun next(): DBOperation? {
            val r = current
            if (reader.filePointer >= metadata.footerStartOffset) {
                current = null
            } else {
                val operation = getNextOperation()
                current = operation
            }
            return r
        }

        fun current(): DBOperation? {
            return current
        }
    }

    fun getNextOperation(): DBOperation? {
        val meta = readKVmeta()

        if (meta == null) return null

        val key = readKey()

        val v: Any

        if (meta.vType == DataType.INT) {
            v = readVInt()
            if (v == -1) return null
        } else {
            v = readString()
        }

        return DBOperation(meta.op, key, v)
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

    private fun readLevel(): Int {
        return reader.read()
    }

    private fun readInt(): Int {
        var v = reader.read()
        v = reader.read() shl 8 or v
        v = reader.read() shl 16 or v
        v = reader.read() shl 24 or v
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

    private fun readString(): String {
        val vLen = readVInt()
        val vBytes = ByteArray(vLen)
        reader.read(vBytes)
        return String(vBytes)
    }

    private fun readKey(): String {
        val prefix = readVInt()
        val kLen = readVInt()
        val kBytes = ByteArray(kLen)
        reader.read(kBytes)
        val currentBytes = lastString.sliceArray(0 until prefix) + kBytes
        val key = String(currentBytes)
        lastString = currentBytes
        return key
    }

    fun readKeyIgnoringKvMeta(): String {
        reader.seek(reader.filePointer + 1)
        return readKey()
    }

    private fun readKVmeta(): KVMetadata? {
        val meta = reader.read()
        if (meta == -1) return null
        return KVMetadata(meta)
    }
}
