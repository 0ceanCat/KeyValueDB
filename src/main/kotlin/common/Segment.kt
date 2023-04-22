package common

import java.util.*

class Segment(
    val path: String,
    val metadata: SegmentMetadata,
    private val sstable: TreeMap<String, Int>
) : Comparable<Segment> {

    override fun equals(other: Any?): Boolean {
        if (other is Segment)
            return path == other.path
        else
            return false
    }

    override fun compareTo(other: Segment): Int {
        return this.id.compareTo(other.id)
    }

    companion object {
        fun overlap(sg1: Segment?, sg2: Segment?): Boolean {
            if (sg1 == null || sg2 == null) return false
            return sg1.firstHigherThan(sg2) and sg1.lastLowerThan(sg2) or sg2.firstHigherThan(sg1) and sg2.lastLowerThan(
                sg1
            )
        }
    }

    val level = metadata.level
    val id = metadata.id

    fun contains(key: String): Boolean {
        val lower = sstable.floorKey(key)
        val higher = sstable.ceilingKey(key)
        return lower != null && higher != null
    }

    private fun firstHigherThan(other: Segment): Boolean {
        return sstable.firstKey().compareTo(other.sstable.firstKey()) >= 0
    }

    private fun lastLowerThan(other: Segment): Boolean {
        return sstable.lastKey().compareTo(other.sstable.lastKey()) >= 0
    }

    override fun toString(): String {
        return "${path}_${level}_${id}"
    }

    fun getStartOffset(key: String): Int {
        return sstable.floorEntry(key).value
    }

    fun getEndOffset(key: String): Int {
        return sstable.ceilingEntry(key)?.value?:Int.MAX_VALUE
    }

    override fun hashCode(): Int {
        return id
    }

}