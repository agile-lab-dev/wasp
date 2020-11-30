package it.agilelab.bigdata.wasp.consumers.spark.plugins.http

import it.agilelab.bigdata.wasp.models.HttpModel
import it.agilelab.bigdata.wasp.core.utils.Utils.using
import com.squareup.okhttp._
import it.agilelab.bigdata.wasp.models.HttpCompression
import org.apache.spark.sql.{ ForeachWriter, Row }
import org.slf4j.LoggerFactory

object HttpWriter {
  def apply(httpModel: HttpModel, bodyColumnName: String): HttpWriter =
    new HttpWriter(
      httpModel.headersFieldName,
      httpModel.url,
      httpModel.mediaType,
      httpModel.method,
      HttpCompression.asString(httpModel.compression),
      httpModel.logBody,
      bodyColumnName
    )
}

class HttpWriter(
  headersFieldName: Option[String],
  url: String,
  mediaType: String,
  method: String,
  compressionStr: String,
  logBody: Boolean,
  bodyColumnName: String
) extends ForeachWriter[Row] {

  private val logger = LoggerFactory.getLogger(this.getClass)

  @transient private lazy val compression = HttpCompression.fromString(compressionStr)

  var okHttpClient: OkHttpClient = _

  override def open(partitionId: Long, epochId: Long): Boolean = {
    okHttpClient = new OkHttpClient()
    true
  }

  /**
    * @param value is a Row with at minimun one column: bodyColumnName that is an Array[Byte] representing the body of
    *              the request already compressed in the format stated by httpModel.compression.
    *              If httpModel.headersFieldName is not None, then it is expected a column of type Map[String, String]
    *              named as headersFieldName.
    *              The only compression types supported are "identity" and "gzip".
    * @throws IllegalArgumentException if set compression type is not supported
    * @throws RuntimeException if response code is not 2xx
    */
  override def process(value: Row): Unit = {

    val header: Option[Headers] =
      headersFieldName.map(headerName => Headers.of(value.getJavaMap[String, String](value.fieldIndex(headerName))))

    val requestBuilder: Request.Builder = new Request.Builder()
      .url(url)

    header.foreach(requestBuilder.headers)

    val requestBody = RequestBody.create(MediaType.parse(mediaType), value.getAs[Array[Byte]](bodyColumnName))

    logB(s"requestBody: $requestBody")

    requestBuilder
      .method(method, requestBody)
      .addHeader(
        "Content-Encoding",
        compression match {
          case HttpCompression.Disabled => "identity"
          case HttpCompression.Gzip     => "gzip"
          case HttpCompression.Snappy   => throw new IllegalArgumentException("Unsupported compression format snappy")
          case HttpCompression.Lz4      => throw new IllegalArgumentException("Unsupported compression format lz4")
        }
      )
    val request = requestBuilder.build()
    logB(s"request: $request")

    val response: Response = okHttpClient.newCall(request).execute()
    val responseBody       = if (logBody) {
      using(response.body()) { b =>
        Some(b.string())
      }
    } else {
      None
    }
    responseBody.foreach(b => logger.info("Body of request: {} \n==========\n{}\n==========", request: Any, b: Any))
    if (response.code() / 100 != 2) {
      responseBody match {
        case Some(b) =>
          throw new RuntimeException(s"Error during http call: ${response.toString} \n==========\n${b}\n==========")
        case None    => throw new RuntimeException(s"Error during http call: ${response.toString}")
      }
    }
  }

  override def close(errorOrNull: Throwable): Unit =
    ()

  private def logB(s: => String) =
    if (logBody) {
      logger.info(s)
    }
}
