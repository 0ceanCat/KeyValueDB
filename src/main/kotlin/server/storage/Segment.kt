package server.storage

import server.bloom.Bloom
import server.writerReader.IndexReader
import java.io.File
import java.util.TreeMap

data class OffsetRange(val start: Long, val end: Long)

class SegmentMetadata(
    val level: Int,
    val id: Int,
    val blocksOffset: TreeMap<String, OffsetRange>,
    val blocksEndOffset: Long,
    val filter: Bloom,
    val lowestKey: String,
    val highestKey: String
)

class Segment(f: File) : Comparable<Segment> {
    val path: String = f.path
    val reader: IndexReader = IndexReader(f)
    val metadata: SegmentMetadata
    private val sstable: TreeMap<String, Block>

    init {
        val reader = IndexReader(f)
        metadata = reader.readMetadata()
        sstable = TreeMap<String, Block>()
        reader.close()
    }

    val level = metadata.level
    val id = metadata.id

    companion object {
        // verify if 2 segments are overlapping
        fun overlap(sg1: Segment?, sg2: Segment?): Boolean {
            if (sg1 == null || sg1.isEmpty() || sg2 == null || sg2.isEmpty()) return false
            return !(sg1.highestKey() < sg2.lowestKey() || sg2.highestKey() < sg1.lowestKey())
        }
    }

    // verify whether the current segment may contain the provided key
    fun mayContain(key: String): Boolean {
        return metadata.filter.contains(key)
    }

    // find the block that may contain the given key
    fun getPossibleBlock(key: String): Block? {
        val entry = sstable.floorEntry(key)
        // block not loaded yet
        var block: Block? = null
        if (entry == null) {
            val offsetRange = metadata.blocksOffset.floorEntry(key).value
            if (offsetRange != null) {
                block = Block.loadBlock(reader, offsetRange.start, offsetRange.end)
                sstable.put(key, block)
                return block
            }
        }
        return block
    }

    override fun equals(other: Any?): Boolean {
        return if (other is Segment)
            path == other.path
        else
            false
    }

    override fun compareTo(other: Segment): Int {
        return this.id.compareTo(other.id)
    }

    override fun toString(): String {
        return "${path}_${level}_${id}"
    }

    override fun hashCode(): Int {
        return id
    }

    private fun lowestKey(): String {
        return metadata.lowestKey
    }

    private fun highestKey(): String {
        return metadata.highestKey
    }

    fun isEmpty(): Boolean {
        return sstable.isEmpty()
    }
}