import common.DBOperation
import common.OperationType
import writerReader.TableWriter
import writerReader.WAL
import writerReader.Writer
import java.io.Closeable
import java.util.TreeMap

class MemoryTable(private val threshold: Int = 1024) : Closeable {
    private val table = TreeMap<String, DBOperation>()
    private val wal: Writer = WAL()
    private val tableWriter: Writer = TableWriter()
    private var size: Int = 0

    fun insert(key: String, v: Any) {
        updateTable(OperationType.INSERT, key, v)
    }

    fun delete(key: String, v: Any) {
        updateTable(OperationType.DELETE, key, v)
    }


    private fun updateTable(op: OperationType, key: String, v: Any) {
        val dbOperation = DBOperation(op, key, v)
        updateSize(key, v)
        writeWAL(dbOperation)
        table[key] = dbOperation
        checkThreshold()
    }

    private fun checkThreshold() {
        if (size >= threshold) {
            writeToDisc()
            reset()
        }
    }

    override fun close() {
        writeToDisc()
        tableWriter.finish()
        wal.finish()
    }

    private fun reset() {
        wal.reset()
        table.clear()
    }

    private fun updateSize(key: String, v: Any) {
        if (key in table) {
            size -= if (table[key]!!.v is Int) 4 else stringSize(table[key]!!.v as String)
        } else {
            size += stringSize(key)
        }
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
        println("write to disc...")
        tableWriter.reset()
        for (entry in table) {
            tableWriter.write(entry.value)
        }
        tableWriter.finish()
    }

}
