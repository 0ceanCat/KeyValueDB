package bloom

import java.lang.Integer.max
import kotlin.math.ceil
import kotlin.math.ln


class Bloom(_n: Int, _p: Double = 0.1, val seed: Long) {
    var k: Int = 0
        // number of hash functions
        private set
    private var m: Int = 0// number of bits
    private var n = _n // number of elements
    private val p = _p // false positive
    private var hashFunctions: UniversalHash? = null
    private var nOfLongInBitMap: Int = 0
    var bitmap: LongArray = longArrayOf()
        private set

    init {
        m = computeBitMapSize()
        k = computeNofHashs()
        nOfLongInBitMap = ceil(m * 1.0 / BITS_LONG).toInt()
        bitmap = LongArray(nOfLongInBitMap)
        m = nOfLongInBitMap * BITS_LONG
        hashFunctions = UniversalHash(k, seed)
    }

    companion object {
        private val BITS_LONG = 64

        fun restore(longArray: LongArray, seed: Long, k: Int): Bloom {
            val filter = Bloom(1, 0.1, seed)
            filter.bitmap = longArray
            filter.m = longArray.size * BITS_LONG
            filter.hashFunctions = UniversalHash(k, seed)
            return filter
        }
    }

    private fun computeNofHashs(): Int {
        return max(1, ((m / n) * ln(2.0)).toInt())
    }

    private fun computeBitMapSize(): Int {
        return (n * ln(p) / ln(1 / Math.pow(2.0, ln(2.0)))).toInt()
    }

    private fun setBitToOne(bitOffset: Int) {
        val longOffset = bitOffset / BITS_LONG
        val bitPos = bitOffset % BITS_LONG
        val long = bitmap[longOffset]
        bitmap[longOffset] = long or (1L shl (BITS_LONG - 1 - bitPos))
    }

    private fun getBit(bitOffset: Int): Long {
        val longOffset = bitOffset / BITS_LONG
        val bitPos = bitOffset % BITS_LONG
        val long = bitmap[longOffset]
        return long and (1L shl (BITS_LONG - 1 - bitPos))
    }

    fun exists(key: String): Boolean {
        for (i in 0 until k) {
            if (getBit(hashFunctions!!.uniHash(key, i) % m) == 0L) return false
        }
        return true
    }

    fun add(key: String) {
        for (i in 0 until k) {
            setBitToOne(hashFunctions!!.uniHash(key, i) % m)
        }
    }
}