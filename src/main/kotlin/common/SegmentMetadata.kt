package common

import kotlin.math.ceil

class SegmentMetadata(val level: Int, val id: Int, _blocksOffset: List<Int>) {
    val blocksOffset: List<Int>

    init {
        computeNofBytesForMetadata()
        var i = 0
        blocksOffset = _blocksOffset.stream().filter{i++ == 0 || it != 0}.toList()
    }

    companion object {
        var numberOfBlocks: Int = -1
            private set
            get() {
                if (field == -1){
                    computeNumberOfBlocks()
                }
                return field
            }

        var nOfbytesForMetadata = -1
            private set
            get() {
                if (field == -1){
                    computeNofBytesForMetadata()
                }
                return field
            }

        val bytesPerBlockOffset = 4
        val bytesForLevel = 1
        val bytesForKVmeta = 1

        private fun computeNumberOfBlocks() {
            numberOfBlocks = ceil(Config.MEMORY_TABLE_THRESHOLD * 1.0 / Config.BLOCK_SIZE).toInt()
        }

        private fun computeNofBytesForMetadata() {
            nOfbytesForMetadata = numberOfBlocks * bytesPerBlockOffset + bytesForLevel
        }
    }
}