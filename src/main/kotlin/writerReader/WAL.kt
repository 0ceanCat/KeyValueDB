package writerReader

import common.DBOperation

class WAL : Writer {
    private val path = "binlog"
    private var writer: Writer = GeneralWriter(path)

    override fun write(op: DBOperation) {
        writer.write(op)
    }

    override fun reset() {
        writer.finish()
        writer = GeneralWriter(path)
    }

    override fun finish() {
        writer.finish()
    }
}