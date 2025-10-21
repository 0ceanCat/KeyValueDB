package server.storage

import common.Utils
import server.writerReader.IndexReader
import server.writerReader.TableWriter
import java.io.File
import java.util.concurrent.locks.ReentrantLock
import kotlin.math.min

object Merger : Thread() {
    private val lock = ReentrantLock()
    private val cond = lock.newCondition()

    init {
        name = "Merger-"
    }

    override fun run() {
        while (true) {
            // get segments to be merged
            var level = 0
            while (level in IndexManager.segmentsByLevel) {
                val segments = IndexManager.segmentsByLevel[level]
                segments?.let {
                    getOverlappedSegments(segments)
                    IndexManager.segmentsByLevel[level + 1]?.let {
                            segmentsOfNextLevel ->
                        for (segment in segments) {
                            findMergeCandidates(segment, segmentsOfNextLevel)
                        }
                    }
                }
                level += 1
            }

            if (!overlaps.isEmpty()) {
                // merge the segments
                for (level in overlaps.keys) {
                    merge(overlaps[level]!!)
                }
            } else {
                // sleep if there are no segments to be merged
                try {
                    lock.lock()
                    cond.await()
                }finally {
                    lock.unlock()
                }
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

    private fun getOverlappedSegments(segments: List<Segment>): List<Segment> {
        val copy = ArrayList(segments)
        copy.sortBy { segments -> segments.metadata.lowestKey }
        var lowestKey = copy.first().lowestKey()
        var highestKey = copy.first().highestKey()

        val overlapped = mutableListOf<Segment>()
        for (segment in segments) {
            if (!(segment.highestKey() < lowestKey || highestKey < segment.lowestKey())) {
                overlapped += segment
                lowestKey = Utils.min(lowestKey, segment.lowestKey())
                highestKey = Utils.min(highestKey, segment.highestKey())
            }
        }
        return overlapped
    }

    private fun findMergeCandidates(segment: Segment, segmentsOfNextLevel: List<Segment>?): List<Segment> {
        if (segmentsOfNextLevel == null) {
            return listOf()
        }

        val candidates = mutableListOf<Segment>()
        for (segmentNextLevel in segmentsOfNextLevel) {
            if (segment.overlaps(segmentNextLevel)) {
                candidates += segmentsOfNextLevel
            }
        }
        if (candidates.isNotEmpty()) {
            candidates += segment
        }
        return candidates
    }

    private fun merge(paths: Set<Segment>) {
        println("merging segments: $paths")
        val readers = mutableListOf<Pair<IndexReader, IndexReader.DBRecordIterator>>()
        for (p in paths) {
            val reader = IndexReader(File(p.path))
            val iterator = reader.iterator() as IndexReader.DBRecordIterator
            readers += Pair(reader, iterator)
        }

        // level of the merged segment
        val level = readers[0].second.metadata.level + 1

        // create the table writer
        val tableWriter = TableWriter(level)
        tableWriter.use {
            while (!readers.isEmpty()) {
                var minRecord = readers[0].second.current()
                var minReader = readers[0]
                for (r in readers) {

                    // if the keys are identical, keep the recent one
                    while (r !== minReader && r.second.current() != null
                        && r.second.current()!!.key == minRecord!!.key
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
                    if (rCurrentRecord != null && rCurrentRecord.key < minRecord!!.key) {
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
        }

        // delete segments
        IndexManager.remove(paths)

        // wake up the Merger
        IndexManager.loadNewSegmentAndNotifyMerger(tableWriter.currentPath)
        println("merge finished...")
    }
}