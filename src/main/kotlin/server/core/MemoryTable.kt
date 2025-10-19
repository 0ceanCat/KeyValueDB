package server.core

import java.util.TreeMap
import kotlin.collections.Map.Entry

class MemoryTable : Iterable<Entry<String, DBRecord>> {
    private val table = TreeMap<String, Pair<DBRecord, Int>>()

    var size = 0
        private set

    fun get(key: String): DBRecord? {
        return table[key]?.first
    }

    fun put(key: String, value: DBRecord) {
        val kvSize = updateSize(key, value.value)
        table[key] = value to kvSize
    }

    private fun updateSize(key: String, v: Any): Int {
        val initial = size
        if (key in table) {
            size -= if (table[key]?.first?.value is Int) vIntSize(v as Int) else stringSize(table[key]?.first?.value as ByteArray)
        } else {
            size += stringSize(key.toByteArray(Charsets.UTF_8))
        }
        size += if (v is Int) {
            vIntSize(v)
        } else {
            stringSize(v as ByteArray)
        }
        return size - initial
    }

    private fun vIntSize(v: Int): Int {
        if (v >= 0x0 && v <= 0x7f) return 1
        else if (v >= 0x80 && v <= 0x3fff) return 2
        else if (v >= 0x4000 && v <= 0x1fffff) return 3
        else if (v >= 0x200000 && v <= 0x0fffffff) return 4
        return 5
    }

    private fun stringSize(v: ByteArray): Int {
        return v.size + vIntSize(v.size)
    }

    override fun iterator(): Iterator<Entry<String, DBRecord>> {
        return TableIterator()
    }

    private inner class TableIterator: Iterator<Entry<String, DBRecord>> {
        val iterator = table.iterator()
        override fun hasNext(): Boolean {
            return iterator.hasNext()
        }

        override fun next(): Entry<String, DBRecord> {
            val next = iterator.next()
            return object : Entry<String, DBRecord> {
                override val key: String
                    get() = next.key
                override val value: DBRecord
                    get() = next.value.first
            }
        }

    }
}
