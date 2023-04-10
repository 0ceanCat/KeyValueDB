package common

class SegmentMetadata(val level: Int, val id: Int, val first: String, _last: String?) {
    val last: String

    init {
        if (_last == null) last = first
        else last = _last
    }
}