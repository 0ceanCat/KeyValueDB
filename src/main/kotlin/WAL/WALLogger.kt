package WAL

import OperationType
import java.io.*

class WALLogger(val path: String) {
    lateinit var logWriter: BufferedOutputStream

    init {
        newWriter()
    }

    fun write(op: OperationType, vType: Int, keyLen: Int, keyData: ByteArray, vLen: Int, vData: ByteArray) {
        logWriter.write((op.id shl 4 or vType))
        logWriter.write(keyLen)
        logWriter.write(keyData)
        logWriter.write(vLen)
        logWriter.write(vData)
        logWriter.write(-1)
        logWriter.flush()
    }

    fun reset() {
        deleteOld()
        newWriter()
    }

    private fun deleteOld() {
        logWriter.close()
        File(path).delete();
    }

    private fun newWriter() {
        logWriter = BufferedOutputStream(FileOutputStream(path))
    }

}