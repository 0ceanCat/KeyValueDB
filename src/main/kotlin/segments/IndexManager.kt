package segments

import common.Utils
import java.io.File
import java.util.*
import kotlin.Comparator

object IndexManager {
    private val path = "index"
    private val indexMap = mutableMapOf<Int, TreeSet<Segment>>()
    private val readIndexNames = mutableMapOf<String, Segment>()
    private var inited = false
    private val newLoaded = mutableListOf<Segment>()
    internal val indexes = mutableListOf<Segment>()

    init {
        scan()
    }

    private fun scan(): List<Segment> {
        val res = mutableListOf<Segment>()
        for (f in Utils.readFilesFrom(path) { it.startsWith("segment") }) {
            if (f.path !in readIndexNames) {
                res += loadNewSegment(f)
            }
        }
        if (!inited) {
            indexes.sortWith(Comparator.comparing { x -> -x.id })
            inited = true
        }
        return res
    }

    private fun loadNewSegment(file: File): Segment {
        val segment = Segment(file)
        val map = indexMap.getOrPut(segment.metadata.level) { TreeSet() }
        map.add(segment)
        indexes.add(segment)
        newLoaded += segment
        readIndexNames[file.path] = segment
        return segment
    }

    fun loadNewSegmentAndNotifyMerger(name: String) {
        loadNewSegment(File(name))
        Merger.tryMerge()
    }

    fun getOverlaps(): Set<Segment> {
        val overlaps = mutableSetOf<Segment>()
        for (f in newLoaded) {
            val map = indexMap[f.metadata.level]!!
            val current = readIndexNames[f.path]!!
            val lower: Segment? = map.lower(current)
            val higher: Segment? = map.higher(current)
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
            indexMap[p.level]?.remove(p)
        }
        indexes.removeAll(paths)
    }


}