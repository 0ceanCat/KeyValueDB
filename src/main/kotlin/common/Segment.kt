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
            val key = reader.readKeyIgnoringKvMeta()
            if (i < blocksOffset.size - 1)
                sstable[key] = Block(path, offset, blocksOffset[i + 1])
            else
                sstable[key] = Block(path, offset, metadata.blocksStartOffset)
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

    fun floorKey(key: String): String? {
        return sstable.floorKey(key)
    }

    fun ceilingKey(key: String): String? {
        return sstable.ceilingKey(key)
    }

    companion object {
        fun overlap(sg1: Segment?, sg2: Segment?): Boolean {
            if (sg1 == null || sg2 == null) return false
            return sg1.firstHigherThan(sg2) && sg1.lastLowerThan(sg2) ||
                    sg2.firstHigherThan(sg1) && sg2.lastLowerThan(sg1)
        }
    }

    val level = metadata.level
    val id = metadata.id

    private fun firstHigherThan(other: Segment): Boolean {
        return sstable.firstKey() >= other.sstable.firstKey()
    }

    private fun lastLowerThan(other: Segment): Boolean {
        return sstable.lastKey() <= other.sstable.lastKey()
    }

    override fun toString(): String {
        return "${path}_${level}_${id}"
    }

    override fun hashCode(): Int {
        return id
    }

    fun getPossibleBlock(key: String): Block {
        val lower = sstable.floorEntry(key)
        val higher = sstable.ceilingEntry(key)
        return lower.value
    }

}