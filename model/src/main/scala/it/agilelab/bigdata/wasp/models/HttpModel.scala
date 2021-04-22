package it.agilelab.bigdata.wasp.models
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct.HttpProduct

/** The HttpModel used by HttpWriter to send data with HTTP protocol
 *
 * @param name The httpModel name
 * @param url The url to send the request to
 * @param method The HTTP methods: GET, POST, PUT, PATCH, DELETE
 * @param headersFieldName The name of the DataFrame column to be used as http headers, 
 *                         it must be of type Map[String,String], if None, no header will be
 *                         sent in the request, except for the content-type and content-encoding
 *                         ones
 * @param valueFieldsNames The list of DataFrame columns to be rendered as json in the http
 *                         request body. If the passed list is empty, all the fields, except
 *                         the headers field (if any) will be rendered as a json object. If
 *                         there is only one field, the behaviour is controlled by the 
 *                         structured field
 * @param compression The HttpCompression
 * @param mediaType The format of the request content
 * @param logBody It enable the request body logger
 * @param structured Indicates how the request body will be rendered. The effect of this
 *                   configuration has effect only if the DataFrame contains only one column
 *                   to be sent and only if it is of ArrayType or MapType. 
 *                   If structured is true the array or map will always be enclosed in a 
 *                   json object, otherwise the map or the array will be at the top level
 *                   of the json document.
 *                   Input dataframe:
 *                   {{{
 *                   +---------+
 *                   |  values |
 *                   +---------+
 *                   |[3, 4, 5]|
 *                   +---------+
 *                   }}}
 *                   Request with structured = true
 *                   {{{
 *                   {"values" : [3, 4, 5]}
 *                   }}}
 *                   Request with structured = false
 *                   {{{
 *                   [3, 4, 5]
 *                   }}}
 */
case class HttpModel(
                      override val name: String,
                      url: String,
                      method: String,
                      headersFieldName: Option[String],
                      valueFieldsNames: List[String],
                      compression: HttpCompression,
                      mediaType: String,
                      logBody: Boolean,
                      structured: Boolean = true
) extends DatastoreModel {
  override def datastoreProduct: DatastoreProduct = HttpProduct
}

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