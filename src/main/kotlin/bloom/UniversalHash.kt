package bloom

import java.util.*

class UniversalHash(hashNumber: Int, val seed: Long) {
    private val random: IntArray
    private val p = 1635947
    init {
        random = IntArray(hashNumber + 1)
        getRandomNumber(hashNumber + 1)
    }

    fun uniHash(element: String, index: Int): Int {
        var hashcode = 0
        val bytes = element.toCharArray()
        for (b in bytes)
            hashcode += (random[index] * b.toInt() + random[index + 1]) % p
        return hashcode
    }

    private fun getRandomNumber(hashNumber: Int) {
        val r = Random(seed)
        for (i in 0 until hashNumber) {
            random[i] = r.nextInt(p)
        }
    }

}