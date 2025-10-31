package server.storage

import common.Utils
import server.writerReader.BlocksReader
import server.writerReader.TableWriter
import java.io.File
import java.util.concurrent.CountDownLatch
import java.util.concurrent.locks.ReentrantLock
import java.util.logging.Logger

object Merger : Thread() {
    private val log: Logger = Logger.getLogger(Merger::class.java.name)
    private val lock = ReentrantLock()
    private val cond = lock.newCondition()

    init {
        name = "Merger-"
    }

    private data class OverlappedSegments(var lowestKey: String, var highestKey: String, val segments: MutableList<Segment>) {
        fun overlaps(overlappedSegments: OverlappedSegments): Boolean {
            return !(this.highestKey < overlappedSegments.lowestKey || overlappedSegments.highestKey < this.lowestKey)
        }
    }

    override fun run() {
        while (true) {
            // get segments to be merged
            var level = 0
            while (level in IndexManager.segmentsByLevel) {
                val segments = IndexManager.segmentsByLevel[level]
                segments?.let {
                    val overlappedSegmentsCurrentLevel = getOverlappedSegmentsAtSameLevel(segments)
                    if (overlappedSegmentsCurrentLevel.isNotEmpty()) {
                        IndexManager.segmentsByLevel[level + 1]?.let { segmentsOfNextLevel ->
                            getOverlappedSegmentsWithNextLevel(segmentsOfNextLevel, overlappedSegmentsCurrentLevel)
                        }
                        merge(level + 1, mergeOverlappedSegmentsCross2Levels(overlappedSegmentsCurrentLevel))
                    }
                }
                level += 1
            }

            try {
                lock.lock()
                cond.await()
            } finally {
                lock.unlock()
            }
        }
    }

    private fun mergeOverlappedSegmentsCross2Levels(overlappedSegmentsCurrentLevel: List<OverlappedSegments>): List<OverlappedSegments> {
        var i = 1
        val result = mutableListOf(overlappedSegmentsCurrentLevel.first())
        while (i < overlappedSegmentsCurrentLevel.size - 1) {
            val os1 = result.last()
            val os2 = overlappedSegmentsCurrentLevel[i + 1]
            result += if (os1.overlaps(os2)) {
                result.removeLast()
                OverlappedSegments(
                    Utils.min(os1.lowestKey, os2.lowestKey),
                    Utils.max(os1.highestKey, os2.highestKey),
                    (os1.segments + os2.segments).toMutableList()
                )
            } else {
                os2
            }
            i += 1
        }
        return result
    }

    private fun getOverlappedSegmentsWithNextLevel(
        segmentsOfNextLevel: List<Segment>, overlappedSegmentsCurrentLevel: List<OverlappedSegments>
    ) {
        for (segment in segmentsOfNextLevel) {
            for (overlappedSegments in overlappedSegmentsCurrentLevel) {
                if (segment.overlaps(overlappedSegments.lowestKey, overlappedSegments.highestKey)) {
                    overlappedSegments.segments += segment
                    overlappedSegments.lowestKey = Utils.min(overlappedSegments.lowestKey, segment.lowestKey())
                    overlappedSegments.highestKey = Utils.max(overlappedSegments.highestKey, segment.highestKey())
                    break
                }
            }
        }
    }

    // wake up the thread
    fun tryMerge() {
        try {
            lock.lock()
            cond.signalAll()
        } finally {
            lock.unlock()
        }
    }

    private fun getOverlappedSegmentsAtSameLevel(segments: List<Segment>): List<OverlappedSegments> {
        val segmentsSortedByFirstKey = ArrayList(segments)
        segmentsSortedByFirstKey.sortBy { segments -> segments.metadata.lowestKey }

        val result = mutableListOf<OverlappedSegments>()
        var overlappedSegments =
            OverlappedSegments(segmentsSortedByFirstKey.first().lowestKey(), segmentsSortedByFirstKey.first().highestKey(), mutableListOf())
        for (segment in segmentsSortedByFirstKey) {
            if (!(segment.highestKey() < overlappedSegments.lowestKey || overlappedSegments.highestKey < segment.lowestKey())) {
                overlappedSegments.segments += segment
                overlappedSegments.lowestKey = Utils.min(overlappedSegments.lowestKey, segment.lowestKey())
                overlappedSegments.highestKey = Utils.min(overlappedSegments.highestKey, segment.highestKey())
            } else {
                if (overlappedSegments.segments.size > 1) {
                    // if there are overlapped segments, add them to the result
                    result += overlappedSegments
                }
                overlappedSegments = OverlappedSegments(segment.lowestKey(), segment.highestKey(), mutableListOf())
            }
        }
        if (overlappedSegments.segments.size > 1) {
            // if there are overlapped segments, add them to the result
            result += overlappedSegments
        }
        return result
    }

    private fun merge(targetLevel: Int, overlappedSegmentsList: List<OverlappedSegments>) {
        log.info("merge started from key ${overlappedSegmentsList.first().lowestKey} to ${overlappedSegmentsList.last().highestKey} for total ${overlappedSegmentsList.sumOf { it.segments.size }} segments, merged segments will be at level $targetLevel.")
        val countDownLatch = CountDownLatch(overlappedSegmentsList.size)
        for (overlappedSegments in overlappedSegmentsList) {
            //startVirtualThread {
                val mergedSegmentPath = mergeHelper(targetLevel, overlappedSegments.segments)
                mergedSegmentPath?.let {
                    IndexManager.loadSegment(File(it))
                    // delete segments
                    IndexManager.remove(overlappedSegments.segments)

                }
                countDownLatch.countDown()
            //}
        }
      //  countDownLatch.await()
        log.info("merge finished.")
    }

    private fun mergeHelper(targetLevel: Int, overlappedSegments: MutableList<Segment>): String? {
        overlappedSegments.sortWith(compareBy<Segment> { it.level }.thenByDescending { it.id })

        val readers = mutableListOf<Pair<Int, BlocksReader.DBRecordIterator>>()
        for (segment in overlappedSegments) {
            val reader = segment.getReader()
            val iterator = reader.iterator() as BlocksReader.DBRecordIterator
            iterator.next()
            readers += segment.id to iterator
        }

        if (readers.isEmpty()) {
            return null
        }

        val tableWriter = TableWriter(targetLevel)
        tableWriter.use {
            while (!readers.isEmpty()) {
                val entry = readers.first()
                var minSegmentId = entry.first
                var minIter = entry.second
                var minRecord = minIter.current()

                for ((currentSegId, rIter) in readers) {
                    if (rIter === minIter) {
                        continue
                    }

                    var rRecord = rIter.current()
                    if (rRecord!!.key == minRecord!!.key) {
                        if (currentSegId > minSegmentId) {
                            // same key, but rRecord has a larger segment id, so it is newer
                            minIter.next()
                            minRecord = rRecord
                            minIter = rIter
                            minSegmentId = currentSegId
                        } else {
                            rRecord = rIter.next()
                        }
                    }

                    // the smaller of the two will be written first.
                    rRecord?.let {
                        if (it.key < minRecord.key) {
                            minRecord = it
                            minIter = rIter
                            minSegmentId = currentSegId
                        }
                    }
                }
                // write the smallest record to dick
                minRecord?.let {
                    tableWriter.write(it)
                    minRecord = minIter.next()
                    if (minRecord == null) {
                        readers.removeIf { it.first == (minSegmentId) }
                    }
                }
            }
        }
        return tableWriter.currentPath
    }
}