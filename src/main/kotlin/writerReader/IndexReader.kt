package writerReader

import common.*
import java.io.File
import java.io.RandomAccessFile

class IndexReader(private val f: File): Iterable<DBOperation?> {
    private val reader: RandomAccessFile
    private val fileId: Int

    init {
        reader = RandomAccessFile(f.path, "r")
        fileId = f.name.split("_")[1].toInt()
    }

    fun readMetadata(close: Boolean = true): SegmentMetadata {
        val level = readLevel()
        val lastOffset = readlastOffset()
        val firstOp = getNextOperation()!!
        reader.seek(lastOffset.toLong())
        val lastOP = getNextOperation()
        if (close)
            reader.close()
        return SegmentMetadata(level, fileId, firstOp.k, lastOP?.k)
    }

    override fun iterator(): Iterator<DBOperation?> {
        return DBOperationIterator()
    }

    inner class DBOperationIterator: Iterator<DBOperation?> {
        val metadata = readMetadata(false)
        private var current: DBOperation?

        init {
            reader.seek(5)
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
        val meta = readMeta()

        if (meta == null) return null

        val key = readKey()

        val v: Any

        if (meta.vType == DataType.INT) {
            v = readInt()
            if (v == -1) return null
        } else {
            v = readString()
        }

        return DBOperation(meta.op, key, v)
    }

    fun close(){
        reader.close()
    }
    fun closeAndRemove() {
        close()
        f.delete()
    }

    private fun readLevel(): Int {
        return reader.read()
    }

    private fun readlastOffset(): Int {
        var v = reader.read()
        v = reader.read() shl 8 or v
        v = reader.read() shl 16 or v
        v = reader.read() shl 24 or v
        return v
    }

    private fun readInt(): Int {
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
        val keyLen = readInt()
        val keyBytes = ByteArray(keyLen)
        reader.read(keyBytes)
        return String(keyBytes)
    }

    private fun readKey(): String {
        return readString()
    }

    private fun readMeta(): KVMetadata? {
        val meta = reader.read()
        if (meta == -1) return null
        return KVMetadata(meta)
    }
}
