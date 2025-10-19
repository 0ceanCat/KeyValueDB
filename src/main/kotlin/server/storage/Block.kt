package server.storage

import server.core.DBRecord
import server.writerReader.IndexReader
import java.util.TreeMap
import kotlin.collections.set

class Block(private val blockCache: TreeMap<String, DBRecord>, private val startOffset: Long, private val endOffset: Long){
    companion object {
        fun loadBlock(reader: IndexReader, startOffset: Long, endOffset: Long): Block {
            val blockCache = TreeMap<String, DBRecord>()
            synchronized(reader) {
                reader.seek(startOffset)
                getNextRecord(reader, endOffset)?.let {
                        dbRecord ->
                    blockCache[dbRecord.key] = dbRecord
                }
                return Block(blockCache, startOffset, endOffset)
            }
        }

        // read next record from the block
        private fun getNextRecord(reader: IndexReader, endOffset: Long): DBRecord? {
            // out of the block
            if (reader.getFilePointer() >= endOffset) return null;
            return reader.getNextRecord()
        }
    }

    // return the corresponding value if the given key is found in cache
    fun get(key: String): DBRecord? {
        return blockCache[key]
    }
}