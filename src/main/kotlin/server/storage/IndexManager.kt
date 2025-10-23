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

    fun remove(paths: List<Segment>) {
        for (p in paths) {
            segmentsByLevel[p.level]?.remove(p)
        }
    }
}