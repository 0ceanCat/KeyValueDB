package server.writerReader

import server.bloom.Bloom
import server.core.DBRecord
import server.core.KVMetadata
import server.enums.DataType
import server.storage.Block
import server.storage.OffsetRange
import server.storage.SegmentMetadata
import java.io.Closeable
import java.io.File
import java.io.FileInputStream
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.util.TreeMap

fun ByteBuffer.readVInt(): Int {
    return readVLong().toInt()
}

fun ByteBuffer.readVLong(): Long {
    var readN = get().toLong()
    // the highest bit indicates if there are more bytes to be read
    // so, we only need the last 7 bits
    // 0x7f == 1111111
    var v = readN and 0x7f

    var shift = 7
    // 0x80 == 10000000
    while ((readN and 0x80) != 0L) {
        readN = get().toLong()
        v = v or ((readN and 0x7F) shl shift)
        shift += 7
    }

    return v
}

fun ByteBuffer.readByte(): Int {
    return get().toInt()
}

fun ByteBuffer.readLong(): Long {
    var v = 0L
    for (i in 0 until Long.SIZE_BYTES) {
        val u8 = get().toLong() and 0xFF // remove sign bit
        v = (u8 shl (Byte.SIZE_BITS * i)) or v
    }
    return v
}

fun ByteBuffer.readMode() {
    flip()
}

fun ByteBuffer.writeMode() {
    clear()
}

fun ByteBuffer.readString(): String {
    return String(readStringAsBytes(), Charsets.UTF_8)
}

fun ByteBuffer.readStringAsBytes(): ByteArray {
    val vLen = readVInt()
    val vBytes = ByteArray(vLen)
    get(vBytes)
    return vBytes
}

fun FileInputStream.readVLong(): Long {
    var readN = read().toLong()
    // the highest bit indicates if there are more bytes to be read
    // so, we only need the last 7 bits
    // 0x7f == 1111111
    var v = readN and 0x7f

    var shift = 7
    // 0x80 == 10000000
    while ((readN and 0x80) != 0L) {
        readN = read().toLong()
        v = v or ((readN and 0x7F) shl shift)
        shift += 7
    }

    return v
}

fun FileInputStream.readVInt(): Int {
    return readVLong().toInt()
}

open class MetadataReader(private val fChannel: FileChannel) {
    fun readMetadata(): SegmentMetadata {
        val smallBuffer = ByteBuffer.allocate(3 * Long.SIZE_BYTES)
        fChannel.read(smallBuffer)
        smallBuffer.readMode()
        val level = smallBuffer.readByte()
        val fileId = smallBuffer.readVInt()


        smallBuffer.writeMode()
        val footerOffset = fChannel.size() - 3 * Long.SIZE_BYTES
        fChannel.read(smallBuffer, footerOffset)

        smallBuffer.readMode()
        val keyRangeOffset = smallBuffer.readLong()
        val blocksIndexOffset = smallBuffer.readLong()
        val filterOffset = smallBuffer.readLong()

        val bigBuffer = ByteBuffer.allocate((footerOffset - keyRangeOffset).toInt())
        fChannel.read(bigBuffer, keyRangeOffset)

        bigBuffer.readMode()
        val firstKey = bigBuffer.readString()
        val lastKey = bigBuffer.readString()

        val blocksIndex = readBlocksIndex(bigBuffer, (filterOffset - blocksIndexOffset).toInt(), keyRangeOffset)

        // read filter
        val bloom = readFilter(bigBuffer)

        return SegmentMetadata(
            level, fileId, blocksIndex, keyRangeOffset, bloom, firstKey, lastKey
        )
    }

    private fun readBlocksIndex(byteBuffer: ByteBuffer, blockIndexSize: Int, lastBlockEndOffset: Long): TreeMap<String, OffsetRange> {
        val blocksIndex = TreeMap<String, OffsetRange>()
        val blocksOffsetList = mutableListOf<Pair<String, Long>>()
        val blockIndexEndOffset = byteBuffer.position() + blockIndexSize
        while (byteBuffer.position() < blockIndexEndOffset) {
            val key = byteBuffer.readString()
            blocksOffsetList += key to byteBuffer.readVLong()
        }
        for (i in 0 until blocksOffsetList.size - 1) {
            val (key, offset) = blocksOffsetList[i]
            val (_, startOffset) = blocksOffsetList[i + 1]
            blocksIndex[key] = OffsetRange(offset, startOffset - 1)
        }
        val last = blocksOffsetList.last()
        blocksIndex[last.first] = OffsetRange(last.second, lastBlockEndOffset - 1)
        return blocksIndex
    }


    private fun readFilter(byteBuffer: ByteBuffer): Bloom {
        val seed = byteBuffer.readVInt()
        val k = byteBuffer.readVInt()
        val bitMapSize = byteBuffer.readVInt()
        val bitMap = LongArray(bitMapSize)
        for (i in 0 until bitMapSize) {
            bitMap[i] = byteBuffer.readVLong()
        }
        return Bloom.restore(bitMap, seed.toLong(), k)
    }
}

open class BlockReader(private val fChannel: FileChannel, private val offsetRange: OffsetRange) {
    private var lastString = byteArrayOf()

    fun readAsBlock(): Block {
        val blockCache = TreeMap<String, DBRecord>()
        readAsSequence().iterator().forEach { blockCache[it.key] = it }
        return Block(blockCache, offsetRange)
    }

    fun readAsSequence(): Sequence<DBRecord> {
        val blockBuffer = readBlocksToByteBuffer()

        return generateSequence {
            if (blockBuffer.position() < blockBuffer.limit()) {
                val meta = KVMetadata(blockBuffer.readByte())
                val key = readPrefixSharedString(blockBuffer)
                val v = readValue(blockBuffer, meta)
                DBRecord(meta.op, key, v)
            } else null
        }
    }

    private fun readPrefixSharedString(byteBuffer: ByteBuffer): String {
        val prefixSize = byteBuffer.readVInt()
        val bytes = byteBuffer.readStringAsBytes()
        val currentBytes = lastString.sliceArray(0 until prefixSize) + bytes
        val key = String(currentBytes)
        lastString = currentBytes
        return key
    }

    private fun readValue(byteBuffer: ByteBuffer, meta: KVMetadata): Any {
        return if (meta.vType == DataType.INT) {
            byteBuffer.readVInt()
        } else {
            byteBuffer.readStringAsBytes()
        }
    }


    private fun readBlocksToByteBuffer(): ByteBuffer {
        val (startOffset, endOffset) = offsetRange
        val blockBuffer = ByteBuffer.allocate((endOffset - startOffset + 1).toInt())
        fChannel.read(blockBuffer, startOffset)
        blockBuffer.readMode()
        return blockBuffer
    }
}

open class BlocksReader(private val fChannel: FileChannel, private val blocksOffsets: List<OffsetRange>) : Iterable<DBRecord?>{
    override fun iterator(): Iterator<DBRecord?> {
        return DBRecordIterator(blocksOffsets)
    }

    inner class DBRecordIterator(private val blocksOffsets: List<OffsetRange>) : Iterator<DBRecord?> {
        init {
            blocksOffsets.sortedBy { it.start }
        }

        private var current: DBRecord? = null
        private var blockIndex = 0
        private var blockIter = BlockReader(fChannel, blocksOffsets[blockIndex]).readAsSequence().iterator()

        override fun hasNext(): Boolean {
            return blockIter.hasNext() || blockIndex < blocksOffsets.size - 1
        }

        override fun next(): DBRecord? {
            if (!blockIter.hasNext()) {
                blockIter = BlockReader(fChannel, blocksOffsets[++blockIndex]).readAsSequence().iterator()
            }
            current = blockIter.next()
            return current
        }

        fun current(): DBRecord? {
            return current
        }
    }
}

open class WalReader(file: File): Iterable<DBRecord>, Closeable {
    private val fis = FileInputStream(file)

    override fun iterator(): Iterator<DBRecord> {
        return readAsSequence().iterator()
    }

    fun readAsSequence(): Sequence<DBRecord> {
        return generateSequence {
            val byte = fis.read()
            if (byte == -1) {
                null
            } else {
                val meta = KVMetadata(byte)
                val key = String(readStringAsBytes())
                val v = if (meta.vType == DataType.INT) {
                    fis.readVInt()
                } else {
                    readStringAsBytes()
                }
                DBRecord(meta.op, key, v)
            }
        }
    }

    private fun readStringAsBytes(): ByteArray {
        val kLen = fis.readVInt()
        val kBytes = ByteArray(kLen)
        fis.read(kBytes)
        return kBytes
    }

    override fun close() {
        fis.close()
    }
}

class VerFReader: Closeable {
    companion object {
        private const val VERF = "verf"
    }

    private val fis = FileInputStream("$FOLDER/$VERF")

    fun readSegmentsIds(): Pair<Int, Set<Int>> {
        var oldestVersion = -1
        var oldestSegmentsIds = setOf<Int>()
        var versionAndSegmentsIds = readVersion()
        while (versionAndSegmentsIds != null) {
            if (versionAndSegmentsIds.first > oldestVersion) {
                oldestSegmentsIds = versionAndSegmentsIds.second
                oldestVersion = versionAndSegmentsIds.first
            }
            versionAndSegmentsIds = readVersion()
        }

        return oldestVersion to oldestSegmentsIds
    }

    private fun readVersion(): Pair<Int, Set<Int>>? {
        val result = mutableSetOf<Int>()
        val version = fis.read()
        if (version == -1) {
            return null
        }
        var hasMore = fis.read()
        while (hasMore == '#'.code) {
            result += fis.readVInt()
            hasMore = fis.read()
        }
        return version to result
    }

    override fun close() {
        fis.close()
    }
}