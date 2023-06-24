package writerReader

import common.DBRecord
import enums.DataType
import enums.OperationType
import java.io.Closeable
import java.io.RandomAccessFile
import java.nio.file.Files
import java.nio.file.Path

abstract class GeneralWriter : Closeable {
    companion object {
        val prefix = "index"
    }

    protected var writer: RandomAccessFile? = null
        set(value) {
            field = value
        }


    protected var lastOffset = 0L
        get() = field

    private var lastString: ByteArray = byteArrayOf()

    init {
        if (!Files.exists(Path.of(prefix))) {
            Files.createDirectory(Path.of(prefix))
        }
    }

    // write a record to disk
    fun write(op: DBRecord, sharePrefix: Boolean = true) {
        lastOffset = writer?.filePointer!!
        val vType = if (op.v is Int) DataType.INT else DataType.STRING
        if (vType == DataType.STRING) {
            // the value is a string
            write(op.op, vType, op.k, (op.v as String), sharePrefix)
        } else {
            // the value is an int
            write(op.op, vType, op.k, op.v as Int, sharePrefix)
        }
    }

    abstract fun reset()

    protected fun writeVint(_v: Int) {
        writeVLong(_v.toLong())
    }

    protected fun writeVLong(_v: Long) {
        val writer_ = writer!!
        var v = _v
        while ((v and 0x7F.inv()) != 0L) {
            writer_.write((v and 0x7F or 0x80).toInt())
            v = v ushr 7
        }
        writer_.write(v.toInt())
    }


    protected fun writeKeySharingPrefix(key: String) {
        val bytes = key.toByteArray()
        var sharedPrefix = 0

        for (i in lastString.indices) {
            if (lastString[i] == bytes[i]) sharedPrefix++
            else break
        }

        lastString = bytes

        writeVint(sharedPrefix)
        writeString(bytes.sliceArray(sharedPrefix until bytes.size))
    }

    protected fun writeWithoutSharingPrefix(key: String) {
        val bytes = key.toByteArray()
        lastString = bytes
        writeVint(0)
        writeString(bytes)
    }

    private fun writeString(bytes: ByteArray) {
        val len = bytes.size
        writeVint(len)
        writeBytes(bytes)
    }

    private fun writeBytes(b: ByteArray) {
        writer!!.write(b)
    }

    override fun close() {
        writer?.close()
    }

    private fun writeKvMeta(
        op: OperationType,
        vType: DataType
    ) {
        writer!!.write(op.id + vType.id)
    }

    private fun write(
        op: OperationType,
        vType: DataType,
        k: String,
        v: String,
        sharePrefix: Boolean = true
    ) {
        writeKvMeta(op, vType)
        if (sharePrefix)
            writeKeySharingPrefix(k)
        else
            writeWithoutSharingPrefix(k)
        writeString(v.toByteArray())
    }

    private fun write(
        op: OperationType,
        vType: DataType,
        k: String,
        v: Int,
        sharePrefix: Boolean = true
    ) {
        writeKvMeta(op, vType)
        if (sharePrefix)
            writeKeySharingPrefix(k)
        else
            writeWithoutSharingPrefix(k)
        writeVint(v)
    }

}