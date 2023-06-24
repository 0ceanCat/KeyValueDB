package common

import segments.SegmentMetadata
import java.util.*
import kotlin.collections.HashMap

class MemoryTable : Iterable<MutableMap.MutableEntry<String, DBRecord>> {
    private val table = TreeMap<String, DBRecord>()
    private val sizes = HashMap<String, Int>()

    var size = 0
        private set

    var blocks = -1
        private set
        get() {
            if (field == -1) updateBlocks()
            return field
        }

    private val points = mutableListOf<Int>()

    private var lastPoint = 0

    companion object {
        val sizePerBlock = 16 //1024 * 16 // 16kb
    }

    fun get(key: String): DBRecord? {
        return table[key]
    }

    fun put(key: String, value: DBRecord) {
        val kvSize = updateSize(key, value.v)
        table[key] = value
        sizes[key] = kvSize
    }

    private fun updateSize(key: String, v: Any): Int {
        val initial = size
        if (key in table) {
            size -= if (table[key]?.v is Int) vIntSize(v as Int) else stringSize(table[key]?.v as String)
        } else {
            size += stringSize(key)
        }
        if (v is Int) {
            size += vIntSize(v)
        } else {
            v as String
            size += stringSize(v)
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

    private fun updateBlocks() {
        var orderedSize = 0
        var blockCounter = 0
        var recordCounter = 0
        for (k in table.keys) {
            orderedSize += sizes[k]!!
            recordCounter += SegmentMetadata.bytesForKVmeta
            if (orderedSize - lastPoint >= sizePerBlock) {
                points += lastPoint
                orderedSize += recordCounter
                lastPoint = orderedSize
                blockCounter++
                recordCounter = 0
            }
        }
        blocks = blockCounter
    }

    private fun stringSize(v: String): Int {
        return v.toByteArray().size + vIntSize(v.length)
    }

    override fun iterator(): Iterator<MutableMap.MutableEntry<String, DBRecord>> {
        return table.iterator()
    }
}
