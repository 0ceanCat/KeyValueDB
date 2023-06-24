package common

import segments.SegmentMetadata
import java.util.*
import kotlin.collections.Map.Entry

class MemoryTable : Iterable<Entry<String, DBRecord>> {
    private val table = TreeMap<String, Pair<DBRecord, Int>>()

    var size = 0
        private set

    var nOfblocks = -1
        private set
        get() {
            if (field == -1) updateNofBlocks()
            return field
        }

    private val points = mutableListOf<Int>()

    private var lastPoint = 0

    companion object {
        val sizePerBlock = 16 //1024 * 16 // 16kb
    }

    fun get(key: String): DBRecord? {
        return table[key]?.first
    }

    fun put(key: String, value: DBRecord) {
        val kvSize = updateSize(key, value.v)
        table[key] = value to kvSize
    }

    private fun updateSize(key: String, v: Any): Int {
        val initial = size
        if (key in table) {
            size -= if (table[key]?.first?.v is Int) vIntSize(v as Int) else stringSize(table[key]?.first?.v as String)
        } else {
            size += stringSize(key)
        }
        size += if (v is Int) {
            vIntSize(v)
        } else {
            v as String
            stringSize(v)
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

    private fun updateNofBlocks() {
        var orderedSize = 0
        var blockCounter = 0
        var recordCounter = 0
        for (kv in table) {
            orderedSize += kv.value.second
            recordCounter += SegmentMetadata.bytesForKVmeta
            if (orderedSize - lastPoint >= sizePerBlock) {
                points += lastPoint
                orderedSize += recordCounter
                lastPoint = orderedSize
                blockCounter++
                recordCounter = 0
            }
        }
        nOfblocks = blockCounter
    }

    private fun stringSize(v: String): Int {
        return v.toByteArray().size + vIntSize(v.length)
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
