package common

import java.io.File
import java.util.Collections

class Utils {
    companion object {
        fun readFilesFrom(dirc: String, accept: (String) -> Boolean = { true }): List<File> {
            val res = mutableListOf<File>()
            val directory = File(dirc)
            if (directory.isDirectory) {
                for (file in directory.listFiles()!!) {
                    if (accept(file.name)) {
                        res += file
                    }
                }
            }

            return res
        }
    }
}

fun main() {
    println(Utils.readFilesFrom("./", { it.startsWith("binlog") }))
}
