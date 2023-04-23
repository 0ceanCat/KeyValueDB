package writerReader

import common.*
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
        val blocksStartOffset = readInt()
        val currentOffset = getFilePointer()
        seek(blocksStartOffset.toLong())
        val blocksOffset = mutableListOf<Int>()
        for (i in 1..SegmentMetadata.numberOfBlocks)
            blocksOffset += readInt()
        seek(currentOffset)
        metadata = SegmentMetadata(level, blocksStartOffset, fileId, blocksOffset)
        return metadata as SegmentMetadata
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
            val operation = getNextOperation()
            current = operation
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
            v = readVint()
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

    private fun readVint(): Int {
        var readN = reader.read()
        // the highest bit indicates if there are more bytes to be read
        // so, we only need the last 7 bits
        // 0x7f == 1111111
        var v = readN and 0x7f

        var shift = 7
        // 0x80 == 10000000
        while ((readN and 0x80) != 0) {
            readN = reader.read()
            v = v or ((readN and 0x7F) shl shift)
            shift += 7
        }

        return v
    }

    private fun readString(): String {
        val vLen = readVint()
        val vBytes = ByteArray(vLen)
        reader.read(vBytes)
        return String(vBytes)
    }

    private fun readKey(): String {
        val prefix = readVint()
        val kLen = readVint()
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
