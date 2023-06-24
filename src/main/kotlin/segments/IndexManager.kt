package segments

import common.Utils
import java.io.File
import java.util.*
import kotlin.Comparator

object IndexManager {
    private val path = "index"
    private val segmentByLevel = mutableMapOf<Int, TreeSet<Segment>>() // store segments by their level
    private val readIndexNames = mutableMapOf<String, Segment>() // segment name -> segment object
    private var inited = false
    private val newLoaded = mutableListOf<Segment>() // store the newly added segment files
    internal val indexes = mutableListOf<Segment>() // store all the segment files

    init {
        // load segment files
        scan()
    }

    private fun scan(): List<Segment> {
        val res = mutableListOf<Segment>()
        for (f in Utils.readFilesFrom(path) { it.startsWith("segment") }) { // find all files whose name starts by 'segment'
            if (f.path !in readIndexNames) {
                res += loadSegment(f)
            }
        }
        if (!inited) {
            // sort the segment files by their id if it's loading them for the first time
            indexes.sortWith(Comparator.comparing { x -> -x.id })
            inited = true
        }
        return res
    }

    private fun loadSegment(file: File): Segment {
        val segment = Segment(file)
        // add the segment into the TreeSet corresponding to its level
        val set = segmentByLevel.getOrPut(segment.metadata.level) { TreeSet() }
        set.add(segment)

        // add the segment into lists and map
        indexes.add(segment)
        newLoaded += segment
        readIndexNames[file.path] = segment
        return segment
    }

    // called when a new segment file was written to disk
    fun loadNewSegmentAndNotifyMerger(name: String) {
        loadSegment(File(name)) // load it
        Merger.tryMerge() // wake up the Merger thread
    }

    // find overlapped segment files for merging
    fun getOverlaps(): Set<Segment> {
        val overlaps = mutableSetOf<Segment>()
        for (f in newLoaded) {
            // only merge overlapped segments that are at the same level.
            val map = segmentByLevel[f.metadata.level]
            val current = readIndexNames[f.path]!!

            // look for neighbors
            val lower: Segment? = map?.lower(current)
            val higher: Segment? = map?.higher(current)

            // check if they are overlapped
            if (Segment.overlap(lower, current)) {
                overlaps.add(lower!!)
                overlaps.add(current)
            }
            if (Segment.overlap(higher, current)) {
                overlaps.add(higher!!)
                overlaps.add(current)
            }
        }
        newLoaded.clear()
        return overlaps
    }

    fun remove(paths: Set<Segment>) {
        for (p in paths) {
            readIndexNames.remove(p.path)
            segmentByLevel[p.level]?.remove(p)
        }
        indexes.removeAll(paths)
    }


}