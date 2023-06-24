package segments

import writerReader.IndexReader
import writerReader.TableWriter
import java.io.File
import java.util.concurrent.locks.ReentrantLock

object Merger : Thread() {
    private val lock = ReentrantLock()
    private val cond = lock.newCondition()

    init {
        name = "Merger-"
    }

    override fun run() {
        while (true) {
            // get segments to be merged
            val overlaps = IndexManager.getOverlaps()

            if (overlaps.isEmpty()) {
                // sleep if there are no segments to be merged
                try {
                    lock.lock()
                    cond.await()
                }finally {
                    lock.unlock()
                }
            } else {
                // merge the segments
                merge(overlaps)
            }
        }
    }

    // wake up the thread
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
        val readers = mutableListOf<Pair<IndexReader, IndexReader.DBRecordIterator>>()
        for ( p in paths) {
            val reader = IndexReader(File(p.path))
            val iterator = reader.iterator() as IndexReader.DBRecordIterator
            readers += Pair(reader, iterator)
        }

        // level of the merged segment
        val level = readers[0].second.metadata.level + 1

        // create the table writer
        val tableWriter = TableWriter()
        tableWriter.reset()
        tableWriter.reserveSpaceForHeader()

        while (!readers.isEmpty()) {
            var minRecord = readers[0].second.current()
            var minReader = readers[0]
            for (r in readers) {

                // if the keys are identical, keep the recent one
                while (r !== minReader && r.second.current() != null
                    && r.second.current()!!.k == minRecord!!.k
                ) {
                    if (minReader.second.metadata.id < r.second.metadata.id) {
                        // if id of `r` is higher, then its data is more recent
                        minReader.second.next()
                        minReader = r
                        minRecord = r.second.current()
                    } else {
                        // r is older then minRecord
                        r.second.next()
                    }
                }

                val rCurrentRecord = r.second.current()
                // if the keys are not the same, the smaller of the two will be written first.
                if (rCurrentRecord != null && rCurrentRecord.k < minRecord!!.k) {
                    minRecord = rCurrentRecord
                    minReader = r
                }
            }

            // advance to the next key-value pair
            minReader.second.next()
            if (minReader.second.current() == null) {
                readers -= minReader
                minReader.first.closeAndRemove()
            }

            // write the smallest record to dick
            minRecord?.let { tableWriter.write(it) }
        }
        tableWriter.fillMetadata(level)
        tableWriter.close()

        // delete segments
        IndexManager.remove(paths)

        // wake up the Merger
        IndexManager.loadNewSegmentAndNotifyMerger(tableWriter.currentPath)
        println("merge finished...")
    }

}