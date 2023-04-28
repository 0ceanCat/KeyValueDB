package segments

import enums.OperationType
import writerReader.IndexReader
import java.io.File
import java.util.TreeMap

class Block(val path: String, val startOffset: Int, val endOffset: Int) {
    private val blockCache = TreeMap<String, Any>()
    private var hasMore = true
    private var nextRecordOffset = -1

    fun get(key: String): Any? {
        if (!hasMore) return blockCache[key]
        val cached = blockCache[key]
        if (cached != null) return cached

        val reader = IndexReader(File(path))
        goToNextRecordOffset(reader)
        var dbOp = reader.getNextOperation()
        while (dbOp != null) {
            if (dbOp.k == key) {
                reader.close()
                if (dbOp.op == OperationType.DELETE) return null
                blockCache[dbOp.k] = dbOp.v
                return dbOp.v
            }
            if (reader.getFilePointer() >= endOffset) break;
            dbOp = reader.getNextOperation()
        }
        hasMore = false
        return null
    }

    private fun goToNextRecordOffset(reader: IndexReader) {
        if (nextRecordOffset == -1) reader.seek(startOffset.toLong())
        else reader.seek(nextRecordOffset.toLong())
    }

}