class MetaData(meta: Int) {
    val op: OperationType
    val vType: Int // 0 -> String, 1 -> Int

    init {
        op = OperationType.of(meta shr 4)
        vType = meta and 0xf
    }
}