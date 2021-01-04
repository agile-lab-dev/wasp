package it.agilelab.bigdata.wasp.consumers.spark.plugins.http

import com.squareup.okhttp.mockwebserver.{Dispatcher, MockResponse, RecordedRequest}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.http.HttpTestUtils._
import it.agilelab.bigdata.wasp.consumers.spark.utils.SparkSuite
import it.agilelab.bigdata.wasp.models.{HttpCompression, HttpModel}
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery, StreamingQueryException}
import org.scalatest.tagobjects.Retryable
import org.scalatest.time.Span
import org.scalatest.{FunSuite, Outcome, Retries}

import java.nio.charset.StandardCharsets
import java.util.concurrent.{CountDownLatch, TimeUnit}

case class ByteData(headers: Map[String, String], values: Array[Byte])
case class StringData(headers: Map[String, String], values: String)

class HttpWriterSpec extends FunSuite with SparkSuite with Retries {
  override def withFixture(test: NoArgTest): Outcome = {
    import Span.convertDurationToSpan

    import scala.concurrent.duration._
    if (isRetryable(test))
      withRetry(1.seconds) { super.withFixture(test) } else
      super.withFixture(test)
  }

  test("Test with header and response code equals to 200") {

    withServer(dispatcher) { serverData =>
      val headers = Map("test" -> "testHeader")
      val values  = "arrayByte".getBytes(StandardCharsets.UTF_8)
      val myDf    = ByteData(headers, values)
      val httpModel = HttpModel(
        name = "name",
        url = s"http://localhost:${serverData.port}/post-test",
        method = "POST",
        headersFieldName = Some("headers"),
        valueFieldsNames = List.empty,
        compression = HttpCompression.Disabled,
        mediaType = "text/plain",
        logBody = false
      )
      import spark.implicits._
      val source: MemoryStream[ByteData] = MemoryStream[ByteData](0, spark.sqlContext)
      assert(
        createAndExecuteStreamingQuery(serverData.latch, source, httpModel, processAllAvailable = true, myDf).isEmpty
      )
    }
  }

  test("Test with header and response code equals to 404", Retryable) {

    withServer(dispatcher) { serverData =>
      val headers = Map("test" -> "testHeader")
      val values  = "arrayByte".getBytes(StandardCharsets.UTF_8)

      val myDf = ByteData(headers, values)

      val httpModel = HttpModel(
        name = "name",
        url = s"http://localhost:${serverData.port}/failure-test",
        method = "POST",
        headersFieldName = Some("headers"),
        valueFieldsNames = List.empty,
        compression = HttpCompression.Disabled,
        mediaType = "text/plain",
        logBody = false
      )
      import spark.implicits._
      val source: MemoryStream[ByteData] = MemoryStream[ByteData](0, spark.sqlContext)
      val exception =
        createAndExecuteStreamingQuery(serverData.latch, source, httpModel, processAllAvailable = false, myDf)
      assert(exception.isDefined)
      assert(
        exception.get.cause.getCause.getCause.getMessage startsWith
          "Error during http call: Response{protocol=http/1.1, code=404, message=OK, " +
            s"url=http://localhost:${serverData.port}/failure-test}"
      )

    }
  }

  test("Test without header and response code equals to 200") {

    withServer(dispatcher) { serverData =>
      val values = "arrayByte".getBytes(StandardCharsets.UTF_8)
      val myDf   = ByteData(Map.empty, values)

      val httpModel = HttpModel(
        name = "name",
        url = s"http://localhost:${serverData.port}/post-test_without_headers",
        method = "POST",
        headersFieldName = None,
        valueFieldsNames = List.empty,
        compression = HttpCompression.Disabled,
        mediaType = "text/plain",
        logBody = false
      )
      import spark.implicits._
      val source: MemoryStream[ByteData] = MemoryStream[ByteData](0, spark.sqlContext)
      assert(
        createAndExecuteStreamingQuery(serverData.latch, source, httpModel, processAllAvailable = true, myDf).isEmpty
      )
    }
  }

  test("Test without header and compressed body with response code equals to 200") {

    withServer(dispatcher) { serverData =>
      val values = compress("arrayByte".getBytes(StandardCharsets.UTF_8))
      val myDf   = ByteData(Map.empty, values)

      val httpModel = HttpModel(
        name = "name",
        url = s"http://localhost:${serverData.port}/post-compression",
        method = "POST",
        headersFieldName = None,
        valueFieldsNames = List.empty,
        compression = HttpCompression.Gzip,
        mediaType = "text/plain",
        logBody = false
      )
      import spark.implicits._
      val source: MemoryStream[ByteData] = MemoryStream[ByteData](0, spark.sqlContext)
      val exception =
        createAndExecuteStreamingQuery(serverData.latch, source, httpModel, processAllAvailable = true, myDf)
      assert(exception.isEmpty)
    }
  }

  def dispatcher(latch: CountDownLatch): Dispatcher =
    new Dispatcher {
      override def dispatch(request: RecordedRequest): MockResponse =
        request.getPath match {
          case "/post-test" =>
            val assertion = tapPrint(
              AggregatedAssertion(
                EqualAssertion("POST", request.getMethod),
                EqualAssertion("testHeader", request.getHeader("test")),
                EqualAssertion("arrayByte", request.getBody.readByteString().utf8())
              )
            )
            latch.countDown()
            assertion.toResponse
          case "/post-test_without_headers" =>
            val assertion = tapPrint(
              AggregatedAssertion(
                EqualAssertion("POST", request.getMethod),
                EqualAssertion("arrayByte", request.getBody.readByteString().utf8())
              )
            )
            latch.countDown()
            assertion.toResponse
          case "/post-compression" =>
            val assertion = tapPrint(
              AggregatedAssertion(
                EqualAssertion("POST", request.getMethod),
                EqualAssertion("gzip", request.getHeader("Content-Encoding")),
                EqualAssertion(
                  "arrayByte",
                  new String(decompress(request.getBody.readByteArray()), StandardCharsets.UTF_8)
                )
              )
            )
            latch.countDown()
            assertion.toResponse
          case _ =>
            new MockResponse().setResponseCode(404)
        }
    }

  def createAndExecuteStreamingQuery[A](
      latch: CountDownLatch,
      source: MemoryStream[A],
      httpModel: HttpModel,
      processAllAvailable: Boolean,
      myDf: A*
  ): Option[StreamingQueryException] = {
    val dsw: DataStreamWriter[Row] = source
      .toDF()
      .writeStream
      .foreach(HttpWriter(httpModel, "values"))

    val streamingQuery: StreamingQuery = dsw.start()
    source.addData(myDf: _*)
    latch.await(1, TimeUnit.SECONDS)
    source.stop()
    if (processAllAvailable) {
      streamingQuery.processAllAvailable()
    }
    streamingQuery.stop()
    if (processAllAvailable) {
      streamingQuery.awaitTermination()
    }
    streamingQuery.exception
  }
}
