import WAL.WALLogger
import java.util.TreeMap

class MemoryTable {
    val table: Map<String, Any> = TreeMap()
    val threshold: Int = 1024 * 1024
    val wal: WALLogger = WALLogger("binlog")

}