package writerReader

import common.DBOperation

class WAL : ReusableWriter {
    private var id = 0
    private val basicPath = "segment"
    private var currentPath = ""
    private val path = "binlog"
    private var writer: DisposableWriter = GeneralWriter(path)

    override fun write(op: DBOperation) {
        writer.write(op)
    }

    override fun reset() {
        writer.finish()
        writer = GeneralWriter(path)
    }
}