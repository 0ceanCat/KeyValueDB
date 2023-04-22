package writerReader

import common.Segment
import common.Utils
import java.util.*
import kotlin.Comparator

object IndexManager {
    private val path = "index"
    private val indexMap = mutableMapOf<Int, TreeSet<Segment>>()
    private val readIndexNames = mutableMapOf<String, Segment>()
    val indexes = TreeSet<Segment>(Comparator.comparing { x -> -x.id })

    private fun scan(): List<Segment> {
        val res = mutableListOf<Segment>()
        for (f in Utils.readFilesFrom(path) { it.startsWith("segment") }) {
            if (f.path !in readIndexNames) {
                val segment = Segment(f)
                indexMap.getOrPut(segment.metadata.level) { TreeSet() }
                indexes.add(segment)
                res += segment
                readIndexNames[f.path] = segment
            }
        }
        return res
    }

    fun getOverlaps(): Set<Segment> {
        val overlaps = mutableSetOf<Segment>()
        for (f in scan()) {
            val map = indexMap[f.metadata.level]!!
            val current = readIndexNames[f.path]!!
            val lower: Segment? = map.floor(current)
            val higher: Segment? = map.ceiling(current)
            if (Segment.overlap(lower, current)) {
                overlaps.add(lower!!)
                overlaps.add(current)
            }
            if (Segment.overlap(higher, current)) {
                overlaps.add(higher!!)
                overlaps.add(current)
            }
            map.add(current)
        }
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