import java.security.InvalidParameterException

enum class OperationType(val id: Int) {
    DELETE(0x1), // 0001
    INSERT(0x2), // 0010
    UPDATE(0x3); // 0011

    companion object {
        fun of(id: Int): OperationType {
            return values().firstOrNull() {
                it.id == id
            } ?: throw InvalidParameterException()
        }
    }

}