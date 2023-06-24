package enums

import java.security.InvalidParameterException

enum class OperationType(val id: Byte) {
    DELETE(0x10), // 00010000
    INSERT(0x20); // 00100000

    companion object {
        fun of(id: Int): OperationType {
            return values().firstOrNull() {
                it.id == id.toByte()
            } ?: throw InvalidParameterException()
        }
    }

}