package server.storage

import server.bloom.Bloom
import java.util.TreeMap

class SegmentMetadata(
    val level: Int,
    val id: Int,
    val blocksOffset: TreeMap<String, BlockHolder>,
    val blocksEndOffset: Long,
    val filter: Bloom,
    val lowestKey: String,
    val highestKey: String
) {

}