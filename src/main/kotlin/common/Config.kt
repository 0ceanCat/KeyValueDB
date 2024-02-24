package common

class Config {
    companion object {
        val MEMORY_TABLE_THRESHOLD = 1024 //1024 * 1024 * 2 // 2MB
        val BLOCK_SIZE = 64 //1024 * 64 // 64 KB
        val BLOOM_FILTER_SIZE = 1000
    }
}
