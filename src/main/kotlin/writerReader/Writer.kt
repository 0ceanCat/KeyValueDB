package writerReader

import common.DBOperation

interface Writer {

    fun write(op: DBOperation)

    fun reset()

    fun finish()
}