package server.storage

import common.Utils
import java.io.File
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicInteger

object IndexManager {
    private const val path = "index"
    private val segmentsByLevel = LinkedBlockingQueue<SegmentsRef>() // store segments by their level and version

    init {
        scan()
    }

    private fun scan() {
        val map = mutableMapOf<Int, MutableList<Segment>>()
        for (f in Utils.readFilesFrom(path) { it.startsWith("segment") }) { // find all files whose name starts by 'segment'
            val segment = loadSegment(f)
            map.computeIfAbsent(segment.level) {
                ArrayList()
            }.add(segment)
        }
        for (segments in map.values) {
            segments.sortBy { segment -> segment.id }
        }
        segmentsByLevel.put(SegmentsRef(map))
    }

    fun loadSegment(file: File): Segment {
        return Segment(file)
    }

    // called when a new segment file was written to disk
    fun loadNewSegmentAndNotifyMerger(name: String) {
        loadSegment(File(name)) // load it
        Merger.tryMerge() // wake up the Merger thread
    }

    fun remove(toBeDeleted: List<Segment>) {
        val newVersionMap = SegmentsRef.clone(segmentsByLevel.last())
        for (segment in toBeDeleted) {
            newVersionMap[segment.level]?.remove(segment)
        }
        segmentsByLevel.add(newVersionMap)
    }

    fun getLastVersionSegments(): SegmentsRef {
        return segmentsByLevel.last()
    }
}

data class SegmentsRef(private val segments: Map<Int, MutableList<Segment>>): Map<Int, MutableList<Segment>> by segments {
    private val reference = AtomicInteger(0)

    companion object {
        fun clone(other: SegmentsRef): SegmentsRef {
            return SegmentsRef(HashMap(other.segments))
        }
    }

    private fun ref() {
        reference.incrementAndGet()
    }

    private fun unRef() {
        reference.decrementAndGet()
    }

    fun <T> use(func: (Map<Int, List<Segment>>) -> T): T? {
        ref()
        val result = func(segments)
        unRef()
        return result
    }
}