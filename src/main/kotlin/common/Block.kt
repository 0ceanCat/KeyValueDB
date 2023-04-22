package common

import writerReader.IndexReader
import java.io.File

class Block(val path: String, val startOffset: Int, val endOffset: Int) {
    fun get(key: String): Any? {
        val reader = IndexReader(File(path))
        reader.seek(startOffset.toLong())
        var dbOp = reader.getNextOperation()
        while (dbOp != null) {
            if (dbOp.k == key) {
                if (dbOp.op == OperationType.DELETE) return null
                reader.close()
                return dbOp.v
            }
            if (reader.getFilePointer() >= endOffset) break;
            dbOp = reader.getNextOperation()
        }
        return null
    }
}