package core

import common.Config
import common.DBRecord
import enums.OperationType
import segments.IndexManager
import segments.Searcher
import writerReader.TableWriter
import writerReader.WAL
import java.io.Closeable
import java.util.LinkedList
import java.util.concurrent.Executors

class Database : Closeable {
    private var table = MemoryTable()
    private val immutableTables = LinkedList<MemoryTable>()
    private var wal: WAL = WAL()
    private val discWriter = Executors.newFixedThreadPool(1)
    private val searcher = Searcher()
    private val threshold = Config.MEMORY_TABLE_THRESHOLD
    private val lock = Any()


    fun insert(key: String, v: Any) {
        updateTable(OperationType.INSERT, key, v)
    }

    fun get(key: String): Any? {
        val dbOperation = table.get(key)
        if (dbOperation != null && dbOperation.op == OperationType.DELETE) return null
        if (dbOperation != null && dbOperation.op == OperationType.INSERT) return dbOperation.v

        val v = searchFromImmutableTablesMemTables(key)

        return v ?: searcher.searchFromSStable(key)
    }

    fun delete(key: String) {
        updateTable(OperationType.DELETE, key, 0)
    }

    private fun updateTable(op: OperationType, key: String, v: Any) {
        synchronized(lock){
            val dbOperation = DBRecord(op, key, v)
            writeWAL(dbOperation)
            table.put(key, dbOperation)
            checkThreshold()
        }
    }

    private fun checkThreshold() {
        if (table.size >= threshold) {
            val toBeWritten = table
            table = MemoryTable()
            immutableTables.addLast(toBeWritten)
            val lastWal = wal
            wal = WAL()
            discWriter.execute {
                val path = writeToDisc(toBeWritten)
                IndexManager.loadNewSegmentAndNotifyMerger(path)
                lastWal.close()
                immutableTables.pollFirst()
            }

        }
    }

    override fun close() {
        writeToDisc(table)
        wal.close()
    }

    private fun reset() {
        wal.reset()
    }

    private fun writeWAL(op: DBRecord) {
        wal.write(op)
    }

    private fun writeToDisc(table: MemoryTable): String {
        val tableWriter = TableWriter()
        tableWriter.reset()
        val currentPath = tableWriter.currentPath
        println("write data to ${tableWriter.currentPath}...")
        tableWriter.reserveSpaceForHeader()
        for (entry in table) {
            tableWriter.write(entry.value)
        }
        tableWriter.fillMetadata()
        tableWriter.close()
        println("${tableWriter.currentPath} done")
        return currentPath
    }

    private fun searchFromImmutableTablesMemTables(key: String): Any? {
        for (t in immutableTables) {
            val dbOperation = t.get(key)
            if (dbOperation != null && dbOperation.op == OperationType.DELETE) return null
            if (dbOperation != null) return dbOperation.v
        }
        return null
    }
}
