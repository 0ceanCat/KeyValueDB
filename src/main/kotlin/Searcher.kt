import common.Segment
import writerReader.IndexManager

class Searcher {

    val sstables: MutableList<Segment> = IndexManager.indexes

    fun searchFromDisc(key: String): Any? {
        for ((i, sstable) in sstables.withIndex()) {
            val lower = sstable.floorKey(key)
            val higher = sstable.ceilingKey(key)
            if (lower != null && higher != null) {
                return lookUpKey(sstable, key)
            } else if (lower != null) {
                if (sstable === sstables.last() || sstables[i + 1].floorKey(key) == null)
                    return lookUpKey(sstable, key)
            }
        }
        return null
    }

    private fun lookUpKey(sstable: Segment, key: String): Any? {
        val block = sstable.getPossibleBlock(key)
        return block.get(key)
    }
}