import common.DBOperation
import common.OperationType
import writerReader.TableWriter
import writerReader.WAL
import java.io.Closeable
import java.util.TreeMap
import java.util.concurrent.Executors

class MemoryTable(private val threshold: Int = 1024) : Closeable {
    private var table = TreeMap<String, DBOperation>()
    private var wal: WAL = WAL()
    private val tableWriter: TableWriter = TableWriter()
    private var size: Int = 0
    private val writeWorker = Executors.newFixedThreadPool(1)
    private val merger = Merger()

    constructor(_table: TreeMap<String, DBOperation>, threshold: Int = 1024) : this(threshold) {
        table = _table
    }

    init {
        merger.start()
    }

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
            val toBeWritten = table
            table = TreeMap()
            val lastWal = wal
            wal = WAL()
            writeWorker.execute {
                writeToDisc(toBeWritten)
                lastWal.close()
            }

        }
    }

    override fun close() {
        writeToDisc(table)
        tableWriter.close()
        wal.close()
    }

    private fun reset() {
        wal.reset()
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

    private fun writeToDisc(table: TreeMap<String, DBOperation>) {
        println("write to disc...")
        tableWriter.reset()
        tableWriter.reserveSpaceForMetadata()
        for (entry in table) {
            tableWriter.write(entry.value)
        }
        tableWriter.fillMetadata()
        tableWriter.close()
        merger.tryMerge()
    }

}
