package server.storage

class BlockHolder(private val offset: Long) {
    private var loaded: Boolean = false
    private var block: Block? = null

    fun getBlock(): Block {
        if (block == null) {

        }
    }
}