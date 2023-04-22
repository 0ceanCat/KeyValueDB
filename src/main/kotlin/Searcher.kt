import common.Segment
import writerReader.IndexManager
import java.util.*

class Searcher {

    val sstables: TreeSet<Segment>

    init {
        sstables = IndexManager.indexes
    }

    fun searchFromDisc(key: String): Any? {
        for (sstable in sstables) {
            val block = sstable.getPossibleBlock(key)
            if (block != null) {
                return block.get(key)
            }
        }

        return null
    }
}