package server.writerReader

import server.bloom.Bloom
import server.core.DBRecord
import server.core.KVMetadata
import server.enums.DataType
import server.storage.Block
import server.storage.OffsetRange
import server.storage.SegmentMetadata
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
        v = (get().toLong() shl (Byte.SIZE_BYTES * i)) or v
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
    val kLen = readVInt()
    return String(slice(position(), kLen).array(), Charsets.UTF_8)
}

fun ByteBuffer.readBytes(): ByteArray {
    val vLen = readVInt()
    val vBytes = ByteArray(vLen)
    get(vBytes)
    return vBytes
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
        fChannel.read(arrayOf(smallBuffer), footerOffset.toInt(), smallBuffer.limit())

        smallBuffer.readMode()
        val keyRangeOffset = smallBuffer.readLong()
        val blocksIndexOffset = smallBuffer.readLong()
        val filterOffset = smallBuffer.readLong()

        val bigBuffer = ByteBuffer.allocate((keyRangeOffset - footerOffset).toInt())
        fChannel.read(arrayOf(bigBuffer), keyRangeOffset.toInt(), bigBuffer.limit())

        bigBuffer.readMode()
        val firstKey = bigBuffer.readString()
        val lastKey = bigBuffer.readString()

        val blocksIndex = readBlocksIndex(bigBuffer, (blocksIndexOffset - filterOffset).toInt(), keyRangeOffset - 1)

        // read filter
        val bloom = readFilter(bigBuffer)

        return SegmentMetadata(
            level, fileId, blocksIndex, keyRangeOffset, bloom, firstKey, lastKey
        )
    }

    private fun readBlocksIndex(byteBuffer: ByteBuffer, blockIndexSize: Int, lastBlockEndOffset: Long): TreeMap<String, OffsetRange> {
        val blocksIndex = TreeMap<String, OffsetRange>()
        val blocksOffsetList = mutableListOf<Pair<String, Long>>()
        while (byteBuffer.position() < blockIndexSize) {
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
        val kLen = byteBuffer.readVInt()
        val kBytes = byteBuffer.slice(byteBuffer.position(), kLen).array()
        val currentBytes = lastString.sliceArray(0 until prefixSize) + kBytes
        val key = String(currentBytes)
        lastString = currentBytes
        return key
    }

    private fun readValue(byteBuffer: ByteBuffer, meta: KVMetadata): Any {
        return if (meta.vType == DataType.INT) {
            byteBuffer.readVInt()
        } else {
            byteBuffer.readBytes()
        }
    }


    private fun readBlocksToByteBuffer(): ByteBuffer {
        val (startOffset, endOffset) = offsetRange
        val blockBuffer = ByteBuffer.allocate((endOffset - startOffset).toInt())
        fChannel.read(arrayOf(blockBuffer), startOffset.toInt(), blockBuffer.limit())
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
