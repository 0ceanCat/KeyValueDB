package writerReader

import common.DBOperation
import common.DataType
import common.OperationType
import java.io.Closeable
import java.io.RandomAccessFile
import java.nio.file.Files
import java.nio.file.Path

abstract class GeneralWriter : Writer, Closeable {
    protected var writer: RandomAccessFile? = null
        set(value) {
            field = value
        }

    protected val prefix = "index"

    protected var lastOffset = 0L
        get() = field

    init {
        if (!Files.exists(Path.of(prefix))) {
            Files.createDirectory(Path.of(prefix))
        }
    }

    override fun write(op: DBOperation) {
        lastOffset = writer?.filePointer!!
        val vType = if (op.v is Int) DataType.INT else DataType.STRING
        if (vType == DataType.STRING) {
            write(op.op, vType, op.k, (op.v as String))
        } else {
            write(op.op, vType, op.k, op.v as Int)
        }
    }

    abstract fun reset()

    private fun writeInt(_v: Int) {
        var v = _v
        while (v and 0x7F.inv() != 0) {
            writer!!.write((v and 0x7F or 0x80))
            v = v ushr 7
        }
        writer!!.write(v)
    }

    private fun writeString(s: String) {
        val bytes = s.encodeToByteArray()
        val len = bytes.size
        writeInt(len)
        writeBytes(bytes)
    }

    private fun writeBytes(b: ByteArray) {
        writer!!.write(b)
    }

    override fun close() {
        writer?.close()
    }

    private fun writeTypeInfo(
        op: OperationType,
        vType: DataType
    ) {
        writer!!.write(op.id + vType.id)
    }

    private fun write(
        op: OperationType,
        vType: DataType,
        k: String,
        v: String
    ) {
        writeTypeInfo(op, vType)
        writeString(k)
        writeString(v)
    }

    private fun write(
        op: OperationType,
        vType: DataType,
        k: String,
        v: Int
    ) {
        writeTypeInfo(op, vType)
        writeString(k)
        writeInt(v)
    }

}