package WAL

import DBOperation
import MetaData
import java.io.BufferedInputStream
import java.io.FileInputStream

class WALReader(path: String) : Iterable<DBOperation> {
    val reader: BufferedInputStream

    init {
        reader = BufferedInputStream(FileInputStream(path))
    }

    override fun iterator(): Iterator<DBOperation> {
        TODO("Not yet implemented")
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
        val key = readKey()
        val v: Any
        if (meta.vType == 1) {
            v = readInt()
            if (v == -1) return null
        } else {
            v = readString()
        }
        return DBOperation(meta.op, key, v)
    }

    private fun readInt(): Int {
        val vLen = reader.read()
        val vBytes = reader.readNBytes(vLen)
        if (vLen != vBytes.size) return -1
        var v = 0
        for (vb in vBytes) v = v shl 8 or vb.toInt()
        return v
    }

    private fun readString(): String {
        val keyLen = reader.read()
        val keyBytes = reader.readNBytes(keyLen)
        return String(keyBytes)
    }

    private fun readKey(): String {
        return readString()
    }

    private fun readMeta(): MetaData {
        val meta = reader.read()
        return MetaData(meta)
    }
}
