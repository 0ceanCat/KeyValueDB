package common

import enums.OperationType

class DBRecord(val op: OperationType, val k: String, val v: Any) {
    override fun toString(): String {
        return "[Key: $k, Value: $v]"
    }
}