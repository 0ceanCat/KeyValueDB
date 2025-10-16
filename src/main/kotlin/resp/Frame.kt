package resp

import java.io.ByteArrayOutputStream
import java.io.InputStream

val CRLF = "\r\n".toByteArray()
val BULK_SIMBLE = '$'.toByte()
val ARRAY_SIMBLE = '*'.toByte()
val STRING_SIMBLE = '+'.toByte()
val ERROR_SIMBLE = '-'.toByte()
val INTEGER_SIMBLE = ':'.toByte()
val BOOLEAN_SIMBLE = '#'.toByte()

sealed class Frame {
    open fun decode(): ByteArray {
        val buffer = ByteArrayOutputStream()
        when (this) {
            is FBulk -> {
                buffer.write('$'.toInt())
                buffer.write(data.size)
                buffer.write(CRLF)
                buffer.write(data)
                buffer.write(CRLF)
            }

            is FArray -> {
                buffer.write('*'.toInt())
                buffer.write(data.size)
                buffer.write(CRLF)
                for (d in data) {
                    buffer.write(d.decode())
                }
            }

            is FBoolean -> {
                buffer.write('#'.toInt())
                buffer.write(if (data) 1 else 0)
                buffer.write(CRLF)
            }

            is FError -> {
                buffer.write('-'.toInt())
                buffer.write(data.toByteArray())
                buffer.write(CRLF)
            }

            is FInteger -> {
                buffer.write(':'.toInt())
                buffer.write(data)
                buffer.write(CRLF)
            }

            is FString -> {
                buffer.write('+'.toInt())
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
                INTEGER_SIMBLE -> {
                    val line = input.readLineUtf8()
                    return line.toIntOrNull()
                        ?.let { FInteger(it) }
                        ?: FError("Invalid integer: '$line'")
                }

                STRING_SIMBLE -> {
                    val line = input.readLineUtf8()
                    return FString(line)
                }

                BULK_SIMBLE -> {
                    val line = input.readLineUtf8()
                    val size = line.toIntOrNull()
                        ?: return FError("Invalid integer: '$line'")

                    val data = ByteArray(size)
                    var read = 0
                    while (read < size) {
                        val r = input.read(data, read, size - read)
                        if (r == -1) return FError("Connection closed")
                        read += r
                    }
                    // read CRLF
                    input.read()
                    input.read()
                    return FBulk(data)
                }

                ARRAY_SIMBLE -> {
                    val line = input.readLineUtf8()
                    val size = line.toIntOrNull() ?: error("Invalid input: $first$line")
                    if (size == -1) {
                        return FArray(emptyList())
                    }
                    val items = mutableListOf<Frame>()
                    for (i in 0 until size) {
                        items.add(decodedFrom(input))
                    }
                    return FArray(items)
                }

                BOOLEAN_SIMBLE -> {
                    val line = input.readLineUtf8()
                    return FBoolean(
                        when (line) {
                            "1" -> true
                            "0" -> false
                            else -> error("Invalid input: $first$line")
                        }
                    )
                }

                else -> error("Invalid input: $first")
            }
        }

        data class FBulk(val data: ByteArray) : Frame()
        data class FString(val data: String) : Frame()
        data class FError(val data: String) : Frame()
        data class FInteger(val data: Int) : Frame()
        data class FArray(val data: List<Frame>) : Frame()
        data class FBoolean(val data: Boolean) : Frame()
    }

}

fun InputStream.readLineUtf8(): String {
    val buffer = ByteArrayOutputStream()
    var prev = -1
    while (true) {
        val b = this.read()
        if (b == -1) throw IllegalStateException("Connection closed")
        if (prev == '\r'.toInt() && b == '\n'.toInt()) {
            val arr = buffer.toByteArray()
            return arr.copyOf(arr.size - 1).toString(Charsets.UTF_8)
        }
        buffer.write(b)
        prev = b
    }
}