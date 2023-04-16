package writerReader

import common.SegmentMetadata
import common.Utils
import java.io.File
import java.util.TreeSet

object IndexManager {
    private val path = "index"
    private val indexes = mutableMapOf<Int, TreeSet<Segment>>()
    private val readIndexes = mutableSetOf<String>()

    class Segment(
        val path: String, private val metadata: SegmentMetadata
    ) : Comparable<Segment> {
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

        private fun firstHigherThan(other: Segment): Boolean {
            return first.compareTo(other.first) >= 0
        }

        private fun lastLowerThan(other: Segment): Boolean {
            return last.compareTo(other.last) <= 0
        }

        override fun toString(): String {
            return "${path}_${level}_${id}"
        }
    }

    fun scan(): Set<Segment> {
        val overlaps = mutableSetOf<Segment>()
        for( f in Utils.readFilesFrom(path) { it.startsWith("segment") }){
            if (f.path !in readIndexes) {
                readIndexes.add(f.path)
                val reader = IndexReader(f)
                val readMetadata = reader.readMetadata()
                val map = indexes.getOrPut(readMetadata.level){ TreeSet() }

                val current = Segment(f.path.toString(), readMetadata)
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
        }
        return overlaps
    }


}