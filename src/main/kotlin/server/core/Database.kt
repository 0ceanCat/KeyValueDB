package server.core

import common.Command
import common.resp.Frame
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import server.Config
import server.enums.OperationType
import server.storage.IndexManager
import server.storage.Searcher
import server.writerReader.SegmentWriter
import server.writerReader.WALWriter
import server.writerReader.WalReader
import java.io.Closeable
import java.io.File
import java.util.concurrent.LinkedBlockingQueue

fun Boolean.toInt(): Int {
    return if (this) 1 else 0
}

class Database : Closeable {
    private val logger: Logger = LogManager.getLogger(Database::class)
    private var table = MemoryTable()
    private val immutableTables = LinkedBlockingQueue<MemoryTable>()
    private var walWriter: WALWriter = WALWriter()
    private val searcher = Searcher()
    private val threshold = Config.MEMORY_TABLE_THRESHOLD
    private val lock = Any()

    fun executeCmd(cmd: Command): Frame {
        when(cmd){
            is Command.Del -> {
                val deleted = delete(cmd.key)
                return Frame.FInteger(deleted.toInt())
            }
            is Command.Get -> {
                val value = get(cmd.key)
                if (value == null) {
                    return Frame.FString("(nil)")
                }
                return if (value is ByteArray) {
                    Frame.FBulk(value)
                } else {
                    Frame.FInteger(value as Int)
                }
            }
            is Command.Set -> {
                insert(cmd.key, cmd.value)
                return Frame.FString("OK")
            }
            is Command.Unknown -> {
                return Frame.FError("Unknown Command")
            }
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

    fun delete(key: String): Boolean {
        return updateTable(OperationType.DELETE, key, 0) != null
    }

    fun recoverFromWal(wal: File) {
        WalReader(wal).use {
            reader ->
            logger.info("reloading wal from ${wal.name}...")
            for(record in reader) {
                table.put(record.key, record)
            }
        }
    }

    private fun updateTable(op: OperationType, key: String, v: Any): DBRecord? {
        val dbOperation = DBRecord(op, key, v)
        synchronized(lock){
            writeWAL(dbOperation)
            val result = table.put(key, dbOperation)
            if (checkThreshold()) {
                writeTableToDisc()
            }
            return result
        }
    }

    private fun checkThreshold(): Boolean {
        return table.size >= threshold
    }

    private fun writeTableToDisc() {
        val toBeWritten = table
        table = MemoryTable()
        immutableTables.add(toBeWritten)
        val lastWal = walWriter
        walWriter = WALWriter()
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
        walWriter.close()
    }

    private fun writeWAL(op: DBRecord) {
        walWriter.write(op)
    }

    private fun writeToDisc(table: MemoryTable): String {
        SegmentWriter(0).use {
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
