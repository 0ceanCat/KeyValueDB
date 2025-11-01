package server.storage

import server.enums.OperationType


class Searcher {
    // search the given key in the segments
    fun searchFromSStable(key: String): Any? {
        return IndexManager.startSearchIn {
            segmentsByLevel ->
            for (level in segmentsByLevel.keys) {
                val segments = segmentsByLevel[level]!!
                for (segment in segments) {
                    if (segment.mayContain(key)){
                        val v = lookUpKey(segment, key)
                        if (v != null) return@startSearchIn v
                    }
                }
            }
            return@startSearchIn null
        }
    }

    private fun lookUpKey(sstable: Segment, key: String): Any? {
        // get possible block
        val block = sstable.getPossibleBlock(key)

        val record = block?.get(key) ?: return null

        return if (record.op == OperationType.DELETE) null else record.value
    }
}