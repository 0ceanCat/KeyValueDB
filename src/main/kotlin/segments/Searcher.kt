package segments

class Searcher {

    private val sstables: MutableList<Segment> = IndexManager.indexes

    fun searchFromDisc(key: String): Any? {
        for (sstable in sstables) {
            if (sstable.mayContains(key)){
                val v = lookUpKey(sstable, key)
                if (v != null) return v
            }

        }
        return null
    }

    private fun lookUpKey(sstable: Segment, key: String): Any? {
        val block = sstable.getPossibleBlock(key)
        return block.get(key)
    }
}