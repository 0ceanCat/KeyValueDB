package writerReader

import common.DBOperation
import common.DataType
import common.OperationType
import java.io.BufferedOutputStream
import java.io.File
import java.io.FileOutputStream
import java.nio.file.Files
import java.nio.file.Path

class GeneralWriter(val path: String) : Writer {
    private val prefix = "index"
    private val writer: BufferedOutputStream

    init {
        if (!Files.exists(Path.of(prefix))){
            Files.createDirectory(Path.of(prefix))
        }
        writer = BufferedOutputStream(FileOutputStream("$prefix/$path"))
    }
    override fun write(op: DBOperation) {
        val vType = if (op.v is Int) DataType.INT else DataType.STRING
        val keyBytes = op.k.encodeToByteArray()
        if (vType == DataType.STRING) {
            val vBytes = (op.v as String).encodeToByteArray()
            write(op.op, vType, keyBytes.size, keyBytes, vBytes.size, vBytes)
        } else {
            write(op.op, vType, keyBytes.size, keyBytes, op.v as Int)
        }
    }

    override fun reset() {
        TODO("Not yet implemented")
    }

    private fun writeVint(_v: Int) {
        var v = _v
        while (v and 0x7F.inv() != 0) {
            writer.write((v and 0x7F or 0x80))
            v = v ushr 7
        }
        writer.write(v)
    }

    private fun write(
        op: OperationType,
        vType: DataType,
        keyLen: Int,
        keyData: ByteArray,
        vLen: Int,
        vData: ByteArray
    ) {
        writer.write(op.id + vType.id)
        writeVint(keyLen)
        writer.write(keyData)
        writeVint(vLen)
        writer.write(vData)
        writer.flush()
    }

    private fun write(
        op: OperationType,
        vType: DataType,
        keyLen: Int,
        keyData: ByteArray,
        vData: Int
    ) {
        writer.write(op.id + vType.id)
        writeVint(keyLen)
        writer.write(keyData)
        writeVint(vData)
        writer.flush()
    }

    override fun finish() {
        writer.close()
    }

}