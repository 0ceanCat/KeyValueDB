package resp

sealed interface Frame {
    companion object {
        val CRLF: String = "\r\n"
    }


}