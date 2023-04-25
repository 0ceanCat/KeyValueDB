package enums

import java.security.InvalidParameterException

enum class OperationType(val id: Int) {
    DELETE(0x8), // 0001000
    INSERT(0x10), // 0010000
    UPDATE(0x11); // 00110000

    companion object {
        fun of(id: Int): OperationType {
            return values().firstOrNull() {
                it.id == id
            } ?: throw InvalidParameterException()
        }
    }

}