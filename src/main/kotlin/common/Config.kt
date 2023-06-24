package common

class Config {
    companion object {
        val MEMORY_TABLE_THRESHOLD = 32 //1024 * 1024 * 2 // 2MB
        val BLOCK_SIZE = 16 //1024 * 64 // 64 KB
        val BLOOM_FILTER_SIZE = 1000
    }
}
