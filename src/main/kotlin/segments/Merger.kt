package segments

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

        // merge level of the output segment
        val level = readers[0].second.metadata.level + 1

        val tableWriter = TableWriter()
        tableWriter.reset()
        tableWriter.reserveSpaceForMetadata()
        while (!readers.isEmpty()) {
            var minDBOperation = readers[0].second.current()
            var minReader = readers[0]
            for (r in readers) {

                // if the keys are identical, keep the recent one
                while (r !== minReader && r.second.current() != null
                    && r.second.current()!!.k == minDBOperation!!.k
                ) {
                    if (minReader.second.metadata.id < r.second.metadata.id) {
                        // if id of r is higher, then its data is more recent
                        minReader.second.next()
                        minReader = r
                        minDBOperation = r.second.current()
                    } else {
                        // r is older then minReade
                        r.second.next()
                    }
                }

                val rCurrentRecord = r.second.current()
                // if the keys are not the same, the smaller of the two will be written first.
                if (rCurrentRecord != null && rCurrentRecord.k.compareTo(minDBOperation!!.k) < 0) {
                    minDBOperation = rCurrentRecord
                    minReader = r
                }
            }

            // advance to the next key-value pair
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
        IndexManager.loadNewSegmentAndNotifyMerger(tableWriter.currentPath)
        println("merge finished...")
    }

}