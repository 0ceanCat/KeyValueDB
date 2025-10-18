package common.resp

val CRLF = "\r\n".toByteArray()
val BULK_SYMBOL = '$'.code.toByte()
val ARRAY_SYMBOL = '*'.code.toByte()
val STRING_SYMBOL = '+'.code.toByte()
val ERROR_SYMBOL = '-'.code.toByte()
val INTEGER_SYMBOL = ':'.code.toByte()
val BOOLEAN_SYMBOL = '#'.code.toByte()