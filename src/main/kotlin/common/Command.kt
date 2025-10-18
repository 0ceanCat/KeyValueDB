package common

import common.resp.Frame
import common.resp.FrameParser
import kotlin.time.Duration

sealed class Command {
    abstract fun toFrame(): Frame

    companion object {
        fun toCommand(parser: FrameParser): Command {
            val cmd = parser.getNextAsString()
            return when(cmd) {
                "set" -> Set(parser.getNextAsString(), parser.getNextAsBytes())
                "get" -> Get(parser.getNextAsString())
                "del" -> Del(parser.getNextAsString())
                else -> Unknown()
            }
        }
    }

    class Unknown: Command() {
        override fun toFrame(): Frame {
            TODO("Not yet implemented")
        }
    }

    data class Set(val key: String, val value: ByteArray, val expire: Duration?=null): Command() {
        override fun toFrame(): Frame {
            val array = Frame.array()
            array.add("set".toByteArray(Charsets.UTF_8))
            array.add(key.toByteArray(Charsets.UTF_8))
            array.add(value)
            return array
        }

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as Set

            if (key != other.key) return false
            if (!value.contentEquals(other.value)) return false
            if (expire != other.expire) return false

            return true
        }

        override fun hashCode(): Int {
            var result = key.hashCode()
            result = 31 * result + value.contentHashCode()
            result = 31 * result + (expire?.hashCode() ?: 0)
            return result
        }
    }

    data class Del(val key: String): Command() {
        override fun toFrame(): Frame {
            val array = Frame.Companion.array()
            array.add("del".toByteArray(Charsets.UTF_8))
            array.add(key.toByteArray(Charsets.UTF_8))
            return array
        }
    }

    data class Get(val key: String): Command() {
        override fun toFrame(): Frame {
            val array = Frame.Companion.array()
            array.add("get".toByteArray(Charsets.UTF_8))
            array.add(key.toByteArray(Charsets.UTF_8))
            return array
        }
    }

}