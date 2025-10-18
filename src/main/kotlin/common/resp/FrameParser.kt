package common.resp

import common.exception.ProtocolParseException

class FrameParser(frame: Frame) {
    private val frameElements: Iterator<Frame>

    init {
        if (frame !is Frame.FArray) {
            throw ProtocolParseException("An array was expected, but received: $frame")
        }

        frameElements = frame.array.iterator()
    }

    fun getNextAsString(): String {
        if (frameElements.hasNext()) {
            val frame = frameElements.next()
            return when(frame) {
                is Frame.FString -> frame.data
                is Frame.FBulk -> String(frame.data, Charsets.UTF_8)
                else -> throw ProtocolParseException("Expecting an array or a bulk, but got: $frame")
            }
        }
        throw ProtocolParseException("All elements have been consumed.")
    }

    fun getNextAsBytes(): ByteArray {
        if (frameElements.hasNext()) {
            val frame = frameElements.next()
            return when(frame) {
                is Frame.FString -> frame.data.toByteArray(Charsets.UTF_8)
                is Frame.FBulk -> frame.data
                else -> throw ProtocolParseException("Expecting an array or a bulk, but got: $frame")
            }
        }
        throw ProtocolParseException("All elements have been consumed.")
    }
}