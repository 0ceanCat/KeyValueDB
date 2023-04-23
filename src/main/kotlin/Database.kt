import common.Config
import common.DBOperation
import common.OperationType
import common.SegmentMetadata
import writerReader.TableWriter
import writerReader.WAL
import java.io.Closeable
import java.util.LinkedList
import java.util.concurrent.Executors

class Database : Closeable {
    private var table = MemoryTable()
    private val fullTables = LinkedList<MemoryTable>()
    private var wal: WAL = WAL()
    private val tableWriter: TableWriter = TableWriter()
    private val writeWorker = Executors.newFixedThreadPool(1)
    private val searcher = Searcher()
    private val threshold = Config.MEMORY_TABLE_THRESHOLD


    fun insert(key: String, v: Any) {
        updateTable(OperationType.INSERT, key, v)
    }

    fun get(key: String): Any? {
        var v = table.get(key)?.v
        v = v ?: let {
            for (t in fullTables) {
                v = t.get(key)?.v
                if (v != null) return v
            }
            v
        }
        return v ?: searcher.searchFromDisc(key)
    }

    fun delete(key: String, v: Any) {
        updateTable(OperationType.DELETE, key, v)
    }

    private fun updateTable(op: OperationType, key: String, v: Any) {
        val dbOperation = DBOperation(op, key, v)
        writeWAL(dbOperation)
        table.put(key, dbOperation)
        checkThreshold()
    }

    private fun checkThreshold() {
        if (table.size >= threshold) {
            val toBeWritten = table
            table = MemoryTable()
            fullTables.addLast(toBeWritten)
            val lastWal = wal
            wal = WAL()
            writeWorker.execute {
                writeToDisc(toBeWritten)
                Merger.tryMerge()
                lastWal.close()
                fullTables.pollFirst()
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

    private fun writeWAL(op: DBOperation) {
        wal.write(op)
    }

    private fun writeToDisc(table: MemoryTable) {
        tableWriter.reset()
        println("write data to ${tableWriter.currentPath}...")
        tableWriter.reserveSpaceForMetadata()
        for (entry in table) {
            tableWriter.write(entry.value)
        }
        tableWriter.fillMetadata()
        tableWriter.close()
        println("${tableWriter.currentPath} done")
    }

}
