package client

sealed class Command {
    class Set(val key: String, val value: ByteArray) {
        fun toFrame(): Frame {

        }
    }
    class Del(val key: String)
    class Get(val key: String)

}