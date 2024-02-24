package segments

import enums.OperationType

class Searcher {
    private val sstables: MutableList<Segment> = IndexManager.indexes
    private val lock = Any()

    // search the given key in the segments
    fun searchFromSStable(key: String): Any? {
        for (sstable in sstables) {
            if (sstable.mayContain(key)){
                val v = lookUpKey(sstable, key)
                if (v != null) return v
            }

        }
        return null
    }

    private fun lookUpKey(sstable: Segment, key: String): Any? {
        // get possible block
        val block = sstable.getPossibleBlock(key)

        //  first try to find it in cache
        val v = block.readFromCache(key)
        if (v != null) return v

        // read a record
        var record = block.getNextRecord()

        while (record != null) {
            if (record.k == key) {
                // close the reader if found the key
                block.readingFinish()
                return if (record.op == OperationType.DELETE) null else record.v
            }
            // read the next record
            record = block.getNextRecord()
        }
        return null
    }

}