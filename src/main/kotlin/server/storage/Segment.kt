package server.storage

import server.bloom.Bloom
import server.writerReader.BlocksReader
import server.writerReader.MetadataReader
import java.io.Closeable
import java.io.File
import java.io.FileInputStream
import java.util.TreeMap
import java.util.ArrayList

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

class Segment(private val file: File) : Comparable<Segment>, Closeable {
    private val fis: FileInputStream = FileInputStream(file)
    private val sstable: TreeMap<String, Block> = TreeMap<String, Block>()
    val path = file.path
    val metadata: SegmentMetadata = MetadataReader(fis.channel).readMetadata()
    val level = metadata.level
    val id = metadata.id

    companion object {
        const val FILE_PREFIX = "segment"
        fun getIdFromName(name: String): Int {
            return name.split("_")[1].toInt()
        }
    }

    // verify if 2 segments are overlapping
    fun overlaps(lowestKey: String, highestKey: String): Boolean {
        return !(this.highestKey() < lowestKey || highestKey < this.lowestKey())
    }

    // verify whether the current segment may contain the provided key
    fun mayContain(key: String): Boolean {
        return metadata.filter.contains(key)
    }

    // find the block that may contain the given key
    fun getPossibleBlock(key: String): Block? {
        val entry = sstable.floorEntry(key)

        if (entry != null) {
            return entry.value
        }

        var block: Block? = null
        val offsetRange = metadata.blocksOffset.floorEntry(key).value
        if (offsetRange != null) {
            block = Block.loadBlock(fis.channel, offsetRange)
            sstable[key] = block
        }
        return block
    }

    override fun equals(other: Any?): Boolean {
        return if (other is Segment)
            id == other.id
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

    fun lowestKey(): String {
        return metadata.lowestKey
    }

    fun highestKey(): String {
        return metadata.highestKey
    }

    override fun close() {
        fis.close()
    }

    fun remove() {
        file.delete()
    }

    fun getReader(): BlocksReader {
        return BlocksReader(fis.channel, ArrayList(metadata.blocksOffset.values))
    }
}