package writerReader

import common.DBOperation
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.RandomAccessFile

interface Writer {
    fun write(op: DBOperation)

}
