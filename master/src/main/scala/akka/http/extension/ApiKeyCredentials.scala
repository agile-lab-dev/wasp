package akka.http.extension

import akka.http.impl.util.Rendering
import akka.http.scaladsl.model.headers.HttpCredentials
import akka.http.scaladsl.model.HttpCharsets.`UTF-8`
import akka.parboiled2.util.Base64

final case class ApiKeyCredentials(key: String) extends HttpCredentials {
    val cookie: Array[Char] = {
        val bytes = key.getBytes(`UTF-8`.nioCharset)
        Base64.rfc2045.encodeToChar(bytes, false)
    }

    override def render[R <: Rendering](r: R): r.type = r ~~ "ApiKey" ~~ cookie

    override def scheme(): String = "ApiKey"

    override def token(): String = String.valueOf(cookie)

    override def params: Map[String, String] = Map.empty
}
