import common.OperationType
import writerReader.IndexManager
import writerReader.IndexReader
import java.io.File

class Searcher {
    fun searchFromDisc(key: String): Any? {
        val segment = IndexManager.getIndexFor(key)
        if (segment == null) return null

        val startOffset = segment.getStartOffset(key)
        val endOffset = segment.getEndOffset(key)

        val reader = IndexReader(File(segment.path))
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