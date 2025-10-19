package server.core

import server.enums.OperationType


class DBRecord(val op: OperationType, val key: String, val value: Any) {
    override fun toString(): String {
        return "[Key: $key, Value: $value]"
    }
}