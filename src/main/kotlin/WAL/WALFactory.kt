package WAL

class WALFactory {
    companion object {
        val path: String = "binlog"
    }

    fun getWALWriter(): WALLogger {
        return WALLogger(path)
    }

    fun getWALReader(): WALReader {
        return WALReader(path)
    }
}