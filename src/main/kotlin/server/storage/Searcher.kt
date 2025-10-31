package server.storage

import server.enums.OperationType


class Searcher {
    // search the given key in the segments
    fun searchFromSStable(key: String): Any? {
        val segmentRef = IndexManager.getLastVersionSegments()
        return segmentRef.use {
            segmentsByLevel ->
            for (level in segmentsByLevel.keys) {
                val segments = segmentsByLevel[level]!!
                for (segment in segments) {
                    if (segment.mayContain(key)){
                        val v = lookUpKey(segment, key)
                        if (v != null) return@use v
                    }
                }
            }
            return@use null
        }
    }

    private fun lookUpKey(sstable: Segment, key: String): Any? {
        // get possible block
        val block = sstable.getPossibleBlock(key)

        val record = block?.get(key) ?: return null

        return if (record.op == OperationType.DELETE) null else record.value
    }
}