package common

class SegmentMetadata(val level: Int, val id: Int, _blocksOffset: List<Int>) {
    val blockN: Int;
    val blocksOffset: List<Int>

    init {
        blockN = _blocksOffset.size
        val globalOffset = computeBytesForMetadata(blockN)
        blocksOffset = _blocksOffset.stream().map { x -> x + globalOffset }.toList()
    }

    companion object {
        val bytesPerBlockOffset = 4
        val bytesForLevel = 1
        val bytesForNofBlocks = 1
        val bytesForKVmeta = 1
        fun computeBytesForMetadata(metas: List<SegmentMetadata>): Int {
            var totalBlocks = 0
            for (m in metas) {
                totalBlocks += m.blockN
            }

            return computeBytesForMetadata(totalBlocks)
        }

        fun computeBytesForMetadata(blockN: Int): Int {
            return blockN * bytesPerBlockOffset + bytesForNofBlocks + bytesForLevel
        }

    }
}