package segments

import bloom.Bloom

class SegmentMetadata(
    val level: Int, val footerStartOffset: Int,
    val id: Int, _blocksOffset: List<Int>,
    val filter: Bloom
) {
    val blocksOffset: List<Int>

    init {
        var i = 0
        blocksOffset = _blocksOffset.stream().filter { i++ == 0 || it != 0 }.toList()
    }

    companion object {
        val bytesForBlockIndexOffset = 4
        val bytesForLevel = 1
        val bytesForKVmeta = 1
        val nOfbytesForMetadata = bytesForLevel + bytesForBlockIndexOffset
    }
}