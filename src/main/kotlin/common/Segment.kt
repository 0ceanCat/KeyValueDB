package common

import writerReader.IndexReader
import java.io.File
import java.util.*

class Segment(
    f: File
) : Comparable<Segment> {
    val path: String
    val metadata: SegmentMetadata
    private val sstable: TreeMap<String, Block>

    init {
        path = f.path
        val reader = IndexReader(f)
        metadata = reader.readMetadata()
        sstable = TreeMap<String, Block>()

        val blocksOffset = metadata.blocksOffset
        for (i in blocksOffset.indices) {
            val offset = blocksOffset[i]
            reader.seek(offset.toLong())
            val key = reader.readKey()
            if (i < blocksOffset.size - 1)
                sstable[key] = Block(path, offset, blocksOffset[i + 1])
            else
                sstable[key] = Block(path, offset, Int.MAX_VALUE)
        }

        reader.close()
    }

    override fun equals(other: Any?): Boolean {
        if (other is Segment)
            return path == other.path
        else
            return false
    }

    override fun compareTo(other: Segment): Int {
        return this.id.compareTo(other.id)
    }

    companion object {
        fun overlap(sg1: Segment?, sg2: Segment?): Boolean {
            if (sg1 == null || sg2 == null) return false
            return sg1.firstHigherThan(sg2) and sg1.lastLowerThan(sg2) or sg2.firstHigherThan(sg1) and sg2.lastLowerThan(
                sg1
            )
        }
    }

    val level = metadata.level
    val id = metadata.id

    private fun contains(key: String): Boolean {
        val lower = sstable.floorKey(key)
        val higher = sstable.ceilingKey(key)
        return lower != null && higher != null
    }

    private fun firstHigherThan(other: Segment): Boolean {
        return sstable.firstKey().compareTo(other.sstable.firstKey()) >= 0
    }

    private fun lastLowerThan(other: Segment): Boolean {
        return sstable.lastKey().compareTo(other.sstable.lastKey()) >= 0
    }

    override fun toString(): String {
        return "${path}_${level}_${id}"
    }

    override fun hashCode(): Int {
        return id
    }

    fun getPossibleBlock(key: String):Block? {
        if (contains(key)){
            val lower = sstable.floorEntry(key)
            val higher = sstable.ceilingEntry(key)
            if (lower === higher) return lower.value
            return Block(path, lower.value.startOffset, higher.value.startOffset)
        }
        return null
    }

}