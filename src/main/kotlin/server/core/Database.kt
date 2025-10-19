package server.core

import common.Command
import server.Config
import server.enums.OperationType
import server.storage.IndexManager
import server.storage.Searcher
import server.writerReader.TableWriter
import server.writerReader.WAL
import java.io.Closeable
import java.util.concurrent.LinkedBlockingQueue

class Database : Closeable {
    private var table = MemoryTable()
    private val immutableTables = LinkedBlockingQueue<MemoryTable>()
    private var wal: WAL = WAL()
    private val searcher = Searcher()
    private val threshold = Config.MEMORY_TABLE_THRESHOLD
    private val lock = Any()

    fun executeCmd(cmd: Command) {
        when(cmd){
            is Command.Del -> {
                delete(cmd.key)
            }
            is Command.Get -> {
                get(cmd.key)
            }
            is Command.Set -> {
                insert(cmd.key, cmd.value)
            }
            is Command.Unknown -> {}
        }
    }

    fun insert(key: String, v: Any) {
        updateTable(OperationType.INSERT, key, v)
    }

    fun get(key: String): Any? {
        val dbOperation = table.get(key)
        if (dbOperation != null && dbOperation.op == OperationType.DELETE) return null
        if (dbOperation != null && dbOperation.op == OperationType.INSERT) return dbOperation.value

        val v = searchFromImmutableTablesMemTables(key)

        return v ?: searcher.searchFromSStable(key)
    }

    fun delete(key: String) {
        updateTable(OperationType.DELETE, key, 0)
    }

    fun reloadRecordFromWAL(dbRecord: DBRecord) {
        table.put(dbRecord.key, dbRecord)
    }

    private fun updateTable(op: OperationType, key: String, v: Any) {
        val dbOperation = DBRecord(op, key, v)
        synchronized(lock){
            writeWAL(dbOperation)
            table.put(key, dbOperation)
            if (checkThreshold()) {
                writeTableToDisc()
            }
        }
    }

    private fun checkThreshold(): Boolean {
        return table.size >= threshold
    }

    private fun writeTableToDisc() {
        val toBeWritten = table
        table = MemoryTable()
        immutableTables.add(toBeWritten)
        val lastWal = wal
        wal = WAL()
        Thread.startVirtualThread {
            val path = writeToDisc(toBeWritten)
            IndexManager.loadNewSegmentAndNotifyMerger(path)
            lastWal.close()
            lastWal.delete()
            immutableTables.poll()
        }
    }

    override fun close() {
        if (table.size > 0) {
            writeToDisc(table)
        }
        wal.close()
    }

    private fun writeWAL(op: DBRecord) {
        wal.write(op)
    }

    private fun writeToDisc(table: MemoryTable): String {
        TableWriter(0).use {
            tableWriter ->
            return tableWriter.writeTable(table)
        }
    }

    private fun searchFromImmutableTablesMemTables(key: String): Any? {
        for (t in immutableTables) {
            val dbOperation = t.get(key)
            if (dbOperation != null && dbOperation.op == OperationType.DELETE) return null
            if (dbOperation != null) return dbOperation.value
        }
        return null
    }
}
