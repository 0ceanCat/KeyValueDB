package server.storage

import server.enums.OperationType

class Searcher {
    // search the given key in the segments
    fun searchFromSStable(key: String): Any? {
        for (level in IndexManager.segmentsByLevel.keys) {
            val segments = IndexManager.segmentsByLevel[level]!!
            for (segment in segments) {
                if (segment.mayContain(key)){
                    val v = lookUpKey(segment, key)
                    if (v != null) return v
                }
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
            if (record.key == key) {
                // close the reader if found the key
                block.readingFinish()
                return if (record.op == OperationType.DELETE) null else record.value
            }
            // read the next record
            record = block.getNextRecord()
        }
        return null
    }

}