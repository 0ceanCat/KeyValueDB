package writerReader

import common.SegmentMetadata
import common.Utils
import java.io.File
import java.util.TreeSet

object IndexManager {
    private val path = "index"
    private val indexMap = mutableMapOf<Int, TreeSet<Segment>>()
    private val indexes = TreeSet<Segment>(Comparator.comparing { x -> -x.id })
    private val readIndexNames = mutableMapOf<String, Segment>()

    class Segment(
        val path: String, metadata: SegmentMetadata
    ) : Comparable<Segment> {

        override fun equals(other: Any?): Boolean {
            if (other is Segment)
                return path == other.path
            else
                return false
        }
        override fun compareTo(other: Segment): Int {
            if (other.first != first) return other.first.compareTo(first)
            return other.last.compareTo(last)
        }

        companion object {
            fun overlap(sg1: Segment?, sg2: Segment?): Boolean {
                if (sg1 == null || sg2 == null) return false
                return sg1.firstHigherThan(sg2) and sg1.lastLowerThan(sg2) or sg2.firstHigherThan(sg1) and sg2.lastLowerThan(
                    sg1
                )
            }
        }

        val last = metadata.last
        val first = metadata.first
        val level = metadata.level
        val id = metadata.id

        fun contains(key: String): Boolean {
            return first <= key && key <= last
        }

        private fun firstHigherThan(other: Segment): Boolean {
            return first.compareTo(other.first) >= 0
        }

        private fun lastLowerThan(other: Segment): Boolean {
            return last.compareTo(other.last) <= 0
        }

        override fun toString(): String {
            return "${path}_${level}_${id}"
        }

        override fun hashCode(): Int {
            var result = path.hashCode()
            result = 31 * result + last.hashCode()
            result = 31 * result + first.hashCode()
            result = 31 * result + level
            result = 31 * result + id
            return result
        }
    }

    private fun scan(): List<File> {
        val res = mutableListOf<File>()
        for (f in Utils.readFilesFrom(path) { it.startsWith("segment") }) {
            if (f.path !in readIndexNames) {
                res += f
                val reader = IndexReader(f)
                val readMetadata = reader.readMetadata()
                indexMap.getOrPut(readMetadata.level) { TreeSet() }
                val segment = Segment(f.path.toString(), readMetadata)
                indexes.add(segment)
                readIndexNames[f.path] = segment
            }
        }
        return res
    }

    fun getIndexFor(key: String): Segment? {
        for (index in indexes) {
            if (index.contains(key)) return index;
        }
        return null
    }

    fun getOverlaps(): Set<Segment> {
        val overlaps = mutableSetOf<Segment>()
        for (f in scan()) {
            val reader = IndexReader(f)
            val readMetadata = reader.readMetadata()
            val map = indexMap[readMetadata.level]!!
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
        for (p in paths){
            readIndexNames.remove(p.path)
            indexMap[p.level]?.remove(p)
        }
        indexes.removeAll(paths)
    }


}