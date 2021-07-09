package akka.http.extension

import akka.http.impl.util.EnhancedString
import java.nio.charset.StandardCharsets
import java.security.MessageDigest

trait SecurityUtils {
    /** redefined from akka.http.impl.utils since it is private.
      */
    implicit def enhanceString_(s: String): EnhancedString = new EnhancedString(s)

    def hash(input: String): String = MessageDigest
        .getInstance("SHA-256")
        .digest(input.getBytes(StandardCharsets.UTF_8))
        .map("%02X" format _)
        .mkString
}
