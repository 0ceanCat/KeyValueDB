package server.storage

import common.Utils
import java.io.File

object IndexManager {
    private const val path = "index"
    val segmentsByLevel = mutableMapOf<Int, MutableList<Segment>>() // store segments by their level

    init {
        scan()
    }

    private fun scan() {
        for (f in Utils.readFilesFrom(path) { it.startsWith("segment") }) { // find all files whose name starts by 'segment'
             loadSegment(f)
        }
        for (segments in segmentsByLevel.values) {
            segments.sortBy { segment -> segment.level }
        }
    }

    private fun loadSegment(file: File): Segment {
        val segment = Segment(file)
        // add the segment into the TreeSet corresponding to its level
        val set = segmentsByLevel.getOrPut(segment.metadata.level) { ArrayList() }
        set.add(segment)
        return segment
    }

    // called when a new segment file was written to disk
    fun loadNewSegmentAndNotifyMerger(name: String) {
        loadSegment(File(name)) // load it
        Merger.tryMerge() // wake up the Merger thread
    }

    // find overlapped segment files for merging
    fun getOverlaps(): Map<Int, Set<Segment>> {
        /*val overlaps = mutableMapOf<Int, MutableSet<Segment>>()
        for (level in segmentsByLevel.keys) {
            val set = overlaps.getOrPut(level) { mutableSetOf() }
            val segments = segmentsByLevel[level]!!
            for (segment in segments) {
                // look for neighbors
                val lower: Segment? = segments.lower(segment)
                val higher: Segment? = segments.higher(segment)

                // check if they are overlapped
                if (Segment.overlap(lower, segment)) {
                    set.add(lower!!)
                    set.add(segment)
                }
                if (Segment.overlap(higher, segment)) {
                    set.add(higher!!)
                    set.add(segment)
                }
            }
        }
        return overlaps*/
        TODO()
    }

    fun getSegmentsForMerge(): Map<Int, Set<Segment>> {
        /*var level = 0
        val overlaps = mutableMapOf<Int, MutableSet<Segment>>()
        while (segmentsByLevel.contains(level)) {
            val segments: TreeSet<Segment> = segmentsByLevel[level]!!
            for (segment in segments) {
                findOverlappedSegments(segment, segmentsByLevel[level + 1])
            }
        }
        for (level in segmentsByLevel.keys) {
            val set = overlaps.getOrPut(level) { mutableSetOf() }
            val segments = segmentsByLevel[level]!!
            for (segment in segments) {
                // look for neighbors
                val lower: Segment? = segments.lower(segment)
                val higher: Segment? = segments.higher(segment)

                // check if they are overlapped
                if (Segment.overlap(lower, segment)) {
                    set.add(lower!!)
                    set.add(segment)
                }
                if (Segment.overlap(higher, segment)) {
                    set.add(higher!!)
                    set.add(segment)
                }
            }
        }
        return overlaps*/
        TODO()
    }

    private fun findOverlappedSegments(segment: Segment, nextLevelSegments: List<Segment>): List<Segment> {
        /*var minKey: String = segment
        var maxKey: String = null*/
        TODO()
    }

    fun remove(paths: Set<Segment>) {
        for (p in paths) {
            segmentsByLevel[p.level]?.remove(p)
        }
    }
}