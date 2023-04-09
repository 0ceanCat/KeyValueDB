package writerReader

interface ReusableWriter : BasicWriter {
    fun reset()
}