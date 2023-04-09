import common.DBOperation
import common.OperationType
import writerReader.TableWriter
import writerReader.WAL
import writerReader.ReusableWriter
import java.util.TreeMap

class MemoryTable {
    private val table = TreeMap<String, DBOperation>()
    private val threshold: Int = 1024
    private val wal: ReusableWriter = WAL()
    private val tableWriter: ReusableWriter = TableWriter()
    private var size: Int = 0

    fun insert(key: String, v: Any) {
        updateTable(OperationType.INSERT, key, v)
    }

    fun delete(key: String, v: Any) {
        updateTable(OperationType.DELETE, key, v)
    }

    private fun updateTable(op: OperationType, key: String, v: Any) {
        val dbOperation = DBOperation(op, key, v)
        table[key] = dbOperation
        writeWAL(dbOperation)
        checkThreshold(key, v)
    }

    private fun checkThreshold(key: String, v: Any) {
        updateSize(key, v)
        if (size >= threshold) {
            writeToDisc()
        }
        reset()
    }

    private fun reset() {
        tableWriter.reset()
        wal.reset()
        table.clear()
    }

    private fun updateSize(key: String, v: Any) {
        size += stringSize(key)
        if (v is Int) {
            size += 4
        } else {
            v as String
            size += stringSize(key)
        }
    }

    private fun stringSize(v: String): Int {
        return v.length * 2
    }

    private fun writeWAL(op: DBOperation) {
        wal.write(op)
    }

    private fun writeToDisc() {
        for (entry in table) {
            tableWriter.write(entry.value)
        }
    }

}
