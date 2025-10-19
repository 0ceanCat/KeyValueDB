package server.storage

import server.core.DBRecord
import server.writerReader.IndexReader
import java.io.File
import java.util.TreeMap

class Block(var path: String, val startOffset: Int, val endOffset: Int){
    private val blockCache = TreeMap<String, Any>()
    private var reader: IndexReader? = null

    // return the corresponding value if the given key is found in cache
    fun readFromCache(key: String): Any?{
        return blockCache[key]
    }

    // read next record from the block
    fun getNextRecord(): DBRecord? {
        reader?:let {
            reader = IndexReader(File(path))
            reader?.seek(startOffset.toLong())
        }
        val reader_ = reader!!

        // out of the block
        if (reader_.getFilePointer() >= endOffset) return null;

        // read and cache
        val record = reader_.getNextRecord()?.let {
            // cache it
            blockCache[it.key] = it.value
            it
        }
        return record
    }

    fun readingFinish(){
        reader?.close()
        reader = null
    }
}