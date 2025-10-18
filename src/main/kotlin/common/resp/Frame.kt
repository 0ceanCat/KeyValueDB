package common.resp

import common.exception.ProtocolParseException
import common.writeInt
import java.io.ByteArrayOutputStream
import java.io.InputStream

sealed class Frame {
    open fun encode(): ByteArray {
        val buffer = ByteArrayOutputStream()
        when (this) {
            is FBulk -> {
                buffer.write(BULK_SYMBOL.toInt())
                buffer.writeInt(data.size)
                buffer.write(data)
                buffer.write(CRLF)
            }

            is FArray -> {
                buffer.write(ARRAY_SYMBOL.toInt())
                buffer.writeInt(array.size)
                for (d in array) {
                    buffer.write(d.encode())
                }
            }

            is FBoolean -> {
                buffer.write(BULK_SYMBOL.toInt())
                buffer.write(if (data) 1 else 0)
                buffer.write(CRLF)
            }

            is FError -> {
                buffer.write(ERROR_SYMBOL.toInt())
                buffer.write(data.toByteArray())
                buffer.write(CRLF)
            }

            is FInteger -> {
                buffer.write(INTEGER_SYMBOL.toInt())
                buffer.writeInt(data)
            }

            is FString -> {
                buffer.write(STRING_SYMBOL.toInt())
                buffer.write(data.toByteArray())
                buffer.write(CRLF)
            }
        }
        return buffer.toByteArray()
    }

    companion object {
        fun decodedFrom(input: InputStream): Frame {
            val first = input.read()
            when (first.toByte()) {
                INTEGER_SYMBOL -> {
                    val line = input.readLineUtf8()
                    return line.toIntOrNull()
                        ?.let { FInteger(it) }
                        ?: throw ProtocolParseException("Invalid integer: '$line'")
                }

                STRING_SYMBOL -> {
                    val line = input.readLineUtf8()
                    return FString(line)
                }

                BULK_SYMBOL -> {
                    val line = input.readLineUtf8()
                    val size = line.toIntOrNull()
                        ?: throw ProtocolParseException("Invalid integer: '$line'")

                    val data = ByteArray(size)
                    var read = 0
                    while (read < size) {
                        val r = input.read(data, read, size - read)
                        if (r == -1) throw ProtocolParseException("common.Connection closed")
                        read += r
                    }
                    // read CRLF
                    input.read()
                    input.read()
                    return FBulk(data)
                }

                ARRAY_SYMBOL -> {
                    val line = input.readLineUtf8()
                    val size = line.toIntOrNull() ?: return FError("Invalid array size: '$line'")
                    if (size == -1) {
                        return FArray(mutableListOf())
                    }
                    val items = mutableListOf<Frame>()
                    for (i in 0 until size) {
                        items.add(decodedFrom(input))
                    }
                    return FArray(items)
                }

                BOOLEAN_SYMBOL -> {
                    val line = input.readLineUtf8()
                    return FBoolean(
                        when (line) {
                            "1" -> true
                            "0" -> false
                            else -> throw ProtocolParseException("Invalid boolean: '$line'")
                        }
                    )
                }
                else -> throw ProtocolParseException("Invalid symbol: '$first'")
            }
        }

        fun array(): FArray {
            return FArray(mutableListOf())
        }
    }

    data class FBulk(val data: ByteArray) : Frame()
    data class FString(val data: String) : Frame()
    data class FError(val data: String) : Frame()
    data class FInteger(val data: Int) : Frame()
    data class FArray(val array: MutableList<Frame>) : Frame() {
        fun add(data: ByteArray) {
            array.add(FBulk(data))
        }
    }
    data class FBoolean(val data: Boolean) : Frame()
}

private fun InputStream.readLineUtf8(): String {
    val buffer = ByteArrayOutputStream()
    var prev = -1
    while (true) {
        val b = this.read()
        if (b == -1) throw IllegalStateException("Connection closed")
        if (prev == '\r'.code && b == '\n'.code) {
            val arr = buffer.toByteArray()
            return arr.copyOf(arr.size - 1).toString(Charsets.UTF_8)
        }
        buffer.write(b)
        prev = b
    }
}