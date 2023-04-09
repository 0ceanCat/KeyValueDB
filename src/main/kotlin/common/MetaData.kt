package common

import common.OperationType

class MetaData(meta: Int) {
    val op: OperationType
    val vType: DataType // 0 -> String, 1 -> Int

    init {
        op = OperationType.of(meta and 0xf0)
        vType = DataType.of(meta and 0xf)
    }
}