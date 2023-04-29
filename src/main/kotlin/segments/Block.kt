package segments

import enums.OperationType
import writerReader.IndexReader
import java.io.File
import java.util.TreeMap

class Block(val path: String, val startOffset: Int, val endOffset: Int) {
    private val blockCache = TreeMap<String, Any?>()
    private var hasMore = true
    private var nextRecordOffset = -1L

    fun get(key: String): Any? {
        // there are no more key-value pairs to be read in the block
        if (!hasMore) return blockCache[key]

        // if the required key is already loaded
        if (key in blockCache) return blockCache[key]

        val reader = IndexReader(File(path))

        goToNextRecordOffset(reader)

        var dbOp = reader.getNextOperation()
        while (dbOp != null) {
            if (dbOp.k == key) {
                reader.close()
                var v: Any? = dbOp.v
                if (dbOp.op == OperationType.DELETE) v = null
                blockCache[dbOp.k] = v
                return v
            }
            if (reader.getFilePointer() >= endOffset) break;
            dbOp = reader.getNextOperation()

            nextRecordOffset = reader.getFilePointer()
        }
        hasMore = false
        return null
    }

    private fun goToNextRecordOffset(reader: IndexReader) {
        if (nextRecordOffset == -1L) reader.seek(startOffset.toLong())
        else reader.seek(nextRecordOffset)
    }

}