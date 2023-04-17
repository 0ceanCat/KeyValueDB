import common.OperationType
import writerReader.IndexManager
import writerReader.IndexReader
import java.io.File

class Searcher {
    fun searchFromDisc(key: String): Any? {
        val segment = IndexManager.getIndexFor(key)
        if (segment == null) return null

        val reader = IndexReader(File(segment.path))
        for (dbOp in reader) {
            if (dbOp!!.k == key) {
                if (dbOp.op == OperationType.DELETE) return null
                reader.close()
                return dbOp.v
            }
        }
        return null
    }
}