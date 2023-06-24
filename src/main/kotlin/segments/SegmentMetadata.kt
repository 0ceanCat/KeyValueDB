package segments

import bloom.Bloom

class SegmentMetadata(
    val level: Int,
    val footerStartOffset: Int,
    val id: Int,
    val blocksOffset: List<Int>,
    val filter: Bloom
) {

    companion object {
        val bytesForBlockIndexOffset = 4
        val bytesForLevel = 1
        val bytesForKVmeta = 1
        val nOfbytesForMetadata = bytesForLevel + bytesForBlockIndexOffset
    }
}