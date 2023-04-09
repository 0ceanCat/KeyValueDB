package writerReader

import common.DBOperation
import common.DataType
import common.MetaData
import java.io.BufferedInputStream
import java.io.FileInputStream

class TableReader(path: String) : Iterable<DBOperation> {
    val reader: BufferedInputStream

    init {
        reader = BufferedInputStream(FileInputStream(path))
    }

    override fun iterator(): Iterator<DBOperation> {
        return Itr()
    }

    private inner class Itr : Iterator<DBOperation> {
        var current: DBOperation? = null
        override fun hasNext(): Boolean {
            val operation = getNextOperation()
            operation?.let { current = operation }
            return operation != null
        }

        override fun next(): DBOperation {
            return current!!
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
        val keyBytes = reader.readNBytes(keyLen)
        return String(keyBytes)
    }

    private fun readKey(): String {
        return readString()
    }

    private fun readMeta(): MetaData? {
        val meta = reader.read()
        if (meta == -1) return null
        return MetaData(meta)
    }
}
