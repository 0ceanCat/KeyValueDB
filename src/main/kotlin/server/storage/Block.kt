package server.storage

import server.core.DBRecord
import server.writerReader.BlockReader
import java.nio.channels.FileChannel
import java.util.TreeMap

open class Block(private val blockCache: TreeMap<String, DBRecord>, val offsetRange: OffsetRange): Iterable<DBRecord> {
    companion object {
        fun loadBlock(fChannel: FileChannel, offsetRange: OffsetRange): Block {
            return BlockReader(fChannel, offsetRange).readAsBlock()
        }
    }

    // return the corresponding value if the given key is found in cache
    fun get(key: String): DBRecord? {
        return blockCache[key]
    }

    inner class BlockIterator : Iterator<DBRecord> {
        private val iterator = blockCache.values.iterator()

        override fun hasNext(): Boolean {
            return iterator.hasNext()
        }

        override fun next(): DBRecord {
            return iterator.next()
        }
    }

    override fun iterator(): Iterator<DBRecord> {
        return BlockIterator()
    }
}