package common

import kotlin.math.ceil

class SegmentMetadata(val level: Int, val blocksStartOffset: Int, val id: Int, _blocksOffset: List<Int>) {
    val blocksOffset: List<Int>

    init {
        var i = 0
        blocksOffset = _blocksOffset.stream().filter { i++ == 0 || it != 0 }.toList()
    }

    companion object {
        var numberOfBlocks: Int = -1
            private set
            get() {
                if (field == -1) {
                    computeNumberOfBlocks()
                }
                return field
            }

        val bytesForBlockIndexOffset = 4
        val bytesForLevel = 1
        val bytesForKVmeta = 1
        val nOfbytesForMetadata = bytesForLevel + bytesForBlockIndexOffset

        private fun computeNumberOfBlocks() {
            numberOfBlocks = ceil(Config.MEMORY_TABLE_THRESHOLD * 1.0 / Config.BLOCK_SIZE).toInt()
        }

    }
}