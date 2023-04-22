import common.Segment
import common.SegmentMetadata
import writerReader.IndexManager
import writerReader.IndexReader
import writerReader.TableWriter
import java.io.File
import java.util.concurrent.locks.ReentrantLock

object Merger : Thread() {
    private val lock = ReentrantLock()
    private val cond = lock.newCondition()
    override fun run() {
        while (true) {
            val overlaps = IndexManager.getOverlaps()
            if (overlaps.isEmpty()) {
                try {
                    lock.lock()
                    cond.await()
                }finally {
                    lock.unlock()
                }
            } else {
                merge(overlaps)
            }
        }
    }

    fun tryMerge(){
        try {
            lock.lock()
            cond.signalAll()
        }finally {
            lock.unlock()
        }
    }

    private fun merge(paths: Set<Segment>) {
        println("merging segments: $paths")
        val readers = mutableListOf<Pair<IndexReader, IndexReader.DBOperationIterator>>()
        for ( p in paths) {
            val reader = IndexReader(File(p.path))
            val iterator = reader.iterator() as IndexReader.DBOperationIterator
            readers += Pair(reader, iterator)
        }
        val level = readers[0].second.metadata.level + 1
        val tableWriter = TableWriter()
        tableWriter.reset()
        tableWriter.reserveSpaceForMetadata(SegmentMetadata.nOfbytesForMetadata)
        while (!readers.isEmpty()) {
            var minDBOperation = readers[0].second.current()
            var minReader = readers[0]
            for (r in readers) {
                while (r !== minReader && r.second.current() != null
                    && r.second.current()!!.k == minDBOperation!!.k
                ) {
                    if (minReader.second.metadata.id < r.second.metadata.id) {
                        // if r's id is higher, then its data is more recent
                        minReader.second.next()
                        minReader = r
                        minDBOperation = r.second.current()
                    } else {
                        r.second.next()
                    }
                }
                if (r.second.current() != null && r.second.current()!!.k.compareTo(minDBOperation!!.k) < 0) {
                    minDBOperation = r.second.current()
                    minReader = r
                }
            }

            minReader.second.next()
            if (minReader.second.current() == null) {
                readers -= minReader
                minReader.first.closeAndRemove()
            }

            minDBOperation?.let { tableWriter.write(it) }
        }
        tableWriter.fillMetadata(level)
        tableWriter.close()
        IndexManager.remove(paths)
        println("merge finished...")
    }

}