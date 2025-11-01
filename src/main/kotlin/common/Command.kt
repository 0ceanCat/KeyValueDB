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
                "set" -> Set(parser.getNextAsString(), parser.getNext())
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

    data class Set(val key: String, val value: Any, val expire: Duration?=null): Command() {
        override fun toFrame(): Frame {
            val array = Frame.array()
            array.add("set".toByteArray(Charsets.UTF_8))
            array.add(key.toByteArray(Charsets.UTF_8))
            array.add(value)
            return array
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