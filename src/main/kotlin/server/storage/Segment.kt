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

class Segment(f: File) : Comparable<Segment>, Closeable {
    private val path: String = f.path
    private val fis: FileInputStream = FileInputStream(path)
    private val sstable: TreeMap<String, Block> = TreeMap<String, Block>()
    private var inUse = false

    val metadata: SegmentMetadata = MetadataReader(fis.channel).readMetadata()
    val level = metadata.level
    val id = metadata.id

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
        return useReader {
            val entry = sstable.floorEntry(key)
            // block not loaded yet
            var block: Block? = null
            if (entry == null) {
                val offsetRange = metadata.blocksOffset.floorEntry(key).value
                if (offsetRange != null) {
                    block = Block.loadBlock(fis.channel, offsetRange)
                    sstable[key] = block
                    return block
                }
            } else {
                block = entry.value
            }
            return block
        }
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

    fun lowestKey(): String {
        return metadata.lowestKey
    }

    fun highestKey(): String {
        return metadata.highestKey
    }

    override fun close() {
        synchronized(this) {
            fis.close()
        }
    }

    fun getReader(): BlocksReader {
        return BlocksReader(fis.channel, ArrayList(metadata.blocksOffset.values))
    }

    private inline fun <T> useReader(func: () -> T): T? {
        synchronized(this) {
            if (!fis.fd.valid()) {
                return null
            }
            val result = func()
            (this as Object).notifyAll()
            return result
        }
    }
}