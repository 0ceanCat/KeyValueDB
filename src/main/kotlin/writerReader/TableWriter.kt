package writerReader

import common.DBOperation

class TableWriter : Writer {
    private var id = 0
    private val basicPath = "segment"
    private var currentPath = ""
    private lateinit var writer: Writer

    override fun write(op: DBOperation) {
        writer.write(op)
    }

    override fun reset() {
        if (::writer.isInitialized) writer.finish()
        startWrite()
    }

    override fun finish() {
        writer.finish()
    }

    fun startWrite() {
        currentPath = "${basicPath}_$id"
        writer = GeneralWriter(currentPath)
        id++
    }

}
