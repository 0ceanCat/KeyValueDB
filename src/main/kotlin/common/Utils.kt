package common

import java.io.File

class Utils {
    companion object {
        fun readFilesFrom(dirc: String, accept: (String) -> Boolean = { true }): List<File> {
            val res = mutableListOf<File>()
            val directory = File(dirc)
            if (directory.isDirectory) {
                val listFiles = directory.listFiles()
                listFiles?.let{
                    for (file in listFiles) {
                        if (accept(file.name))
                            res.add(file)
                    }
                }
            }

            return res
        }

        fun min(s: String, s2: String): String {
            return if (s < s2) s else s2
        }

        fun max(s: String, s2: String): String {
            return if (s > s2) s else s2
        }
    }
}