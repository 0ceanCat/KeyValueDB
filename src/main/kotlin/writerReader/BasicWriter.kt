package writerReader

import common.DBOperation

interface BasicWriter {
    fun write(op: DBOperation)
}