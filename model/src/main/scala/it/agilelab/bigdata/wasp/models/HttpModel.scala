package it.agilelab.bigdata.wasp.models

import it.agilelab.bigdata.wasp.datastores.HttpCategory

case class HttpModel(
                      override val name: String,
                      url: String,
                      method: String, // GET, POST, PUT, PATCH, DELETE
                      headersFieldName: Option[String],
                      valueFieldsNames: List[String],
                      compression: HttpCompression, // HttpCompression
                      mediaType: String,
                      logBody: Boolean
) extends DatastoreModel[HttpCategory]

sealed abstract class HttpCompression(val codec: String)

object HttpCompression {

  private[wasp] val _asString : Map[HttpCompression, String] = Map(
    HttpCompression.Disabled -> "disabled",
    HttpCompression.Gzip -> "gzip",
    HttpCompression.Snappy -> "snappy",
    HttpCompression.Lz4 -> "lz4"
  )

  def asString : PartialFunction[HttpCompression, String] = _asString

  def fromString: PartialFunction[String, HttpCompression] = _fromString

  private val _fromString: Map[String, HttpCompression] = _asString.map(_.swap)

  case object Disabled extends HttpCompression("none")
  case object Gzip extends HttpCompression("gzip")
  case object Snappy extends HttpCompression("snappy")
  case object Lz4 extends HttpCompression("lz4")

}