package writerReader

import common.DBOperation
class TableWriter: ReusableWriter {
    private var id = 0
    private val basicPath = "segment"
    private var currentPath = ""
    private lateinit var writer: DisposableWriter

    init {
        newWriter()
    }

    override fun write(op: DBOperation) {
        writer.write(op)
    }

    override fun reset() {
        writer.finish()
        newWriter()
    }

    private fun newWriter() {
        currentPath = "${basicPath}_$id"
        writer = GeneralWriter(currentPath)
        id++
    }

}
