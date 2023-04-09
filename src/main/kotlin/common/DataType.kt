package common

import java.security.InvalidParameterException

enum class DataType(val id: Int) {
    INT(0),
    STRING(1);

    companion object {
        fun of(id: Int): DataType {
            return DataType.values().firstOrNull() {
                it.id == id
            } ?: throw InvalidParameterException()
        }
    }
}