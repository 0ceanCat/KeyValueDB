package writerReader

interface DisposableWriter : BasicWriter {
    fun finish()
}