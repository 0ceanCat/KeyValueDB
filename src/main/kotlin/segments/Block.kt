package segments

import common.DBRecord
import writerReader.IndexReader
import java.io.File
import java.util.TreeMap

class Block(val path: String, val startOffset: Int, val endOffset: Int){
    private val blockCache = TreeMap<String, Any>()
    private var nextRecordOffset = -1L
    private var reader: IndexReader? = null

    // return the corresponding value if the given key is found in cache
    fun readFromCache(key: String): Any?{
        return blockCache[key]
    }

    // read next record from the block
    fun getNextRecord(): DBRecord? {
        reader?:let { reader = IndexReader(File(path))  }
        val reader_ = reader!!

        // go to the offset of the next record to be read
        goToNextRecordOffset()

        // out of the block
        if (reader_.getFilePointer() >= endOffset) return null;

        // read and cache
        val record = reader_.getNextRecord()?.let {
            // cache it
            blockCache[it.k] = it.v
            it
        }

        // update the pointer
        nextRecordOffset = reader_.getFilePointer()
        return record
    }

    fun readingFinish(){
        reader?.close()
        nextRecordOffset = 0
        reader = null
    }
    private fun goToNextRecordOffset() {
        if (nextRecordOffset == -1L) reader?.seek(startOffset.toLong()) // when the current block is read for the first time
        else reader?.seek(nextRecordOffset) // go to the next record
    }

}