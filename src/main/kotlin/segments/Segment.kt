package segments

import writerReader.IndexReader
import java.io.File
import java.util.*

class Segment(f: File) : Comparable<Segment> {
    val path: String
    val metadata: SegmentMetadata
    private val sstable: TreeMap<String, Block>

    init {
        path = f.path
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
            if (sg1 == null || sg2 == null) return false
            return !(sg1.lastKey() < sg2.firstKey() || sg2.lastKey() < sg1.firstKey())
        }
    }

    // verify whether the current segment may contain the provided key
    fun mayContain(key: String): Boolean {
        return metadata.filter.contains(key)
    }

    // find the block that may contain the given key
    fun getPossibleBlock(key: String): Block {
        if (sstable.isEmpty()) loadBlocksOffsetFromDisk()
        return sstable.floorEntry(key).value
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

    private fun firstKey(): String {
        loadOnlyFirstAndLastBlocks()
        return sstable.firstKey()
    }

    private fun lastKey(): String {
        loadOnlyFirstAndLastBlocks()
        return sstable.lastKey()
    }

    private fun loadBlocksOffsetFromDisk() {
        val reader = IndexReader(File(path))
        val blocksOffset = metadata.blocksOffset
        for (i in blocksOffset.indices) {
            val offset = blocksOffset[i]
            reader.seek(offset.toLong())
            val key = reader.readKeyIgnoringKvMeta()
            if (i < blocksOffset.size - 1)
                sstable[key] = Block(path, offset, blocksOffset[i + 1])
            else
                sstable[key] = Block(path, offset, metadata.footerStartOffset)
        }
        reader.close()
    }

    private fun loadOnlyFirstAndLastBlocks() {
        if (!sstable.isEmpty()) return

        val reader = IndexReader(File(path))
        val blocksOffset = metadata.blocksOffset

        val firstAndLast = listOf(blocksOffset.first(), blocksOffset.last())
        for ((i, offset) in firstAndLast.withIndex()) {
            reader.seek(offset.toLong())
            val key = reader.readKeyIgnoringKvMeta()
            if (i < firstAndLast.size - 1)
                sstable[key] = Block(path, offset, firstAndLast[0 + 1])
            else {
                sstable[key] = Block(path, offset, metadata.footerStartOffset)
            }
        }
        reader.close()
    }
}