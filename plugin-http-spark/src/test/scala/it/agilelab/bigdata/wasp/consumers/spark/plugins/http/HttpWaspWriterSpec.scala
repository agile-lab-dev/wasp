package it.agilelab.bigdata.wasp.consumers.spark.plugins.http

import java.nio.charset.StandardCharsets
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.squareup.okhttp.mockwebserver.{Dispatcher, MockResponse, RecordedRequest}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.http.HttpTestUtils.{decompress, tapPrint, withServer}
import it.agilelab.bigdata.wasp.consumers.spark.utils.SparkSuite
import it.agilelab.bigdata.wasp.models.{HttpModel, HttpCompression}
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery, StreamingQueryException}
import org.scalatest.FunSuite

class HttpWaspWriterSpec extends FunSuite with SparkSuite {
  test("Test with header and response code equals to 200") {

    withServer(dispatcher) { serverData =>
      val headers   = Map("test" -> "testHeader")
      val values    = "arrayByte"
      val myDf      = StringData(headers, values)
      val httpModel = HttpModel(
        name = "name",
        url = s"http://localhost:${serverData.port}/post-test",
        method = "POST",
        headersFieldName = Some("headers"),
        valueFieldsNames = List("values"),
        compression = HttpCompression.Disabled,
        mediaType = "text/plain",
        logBody = false
      )
      import spark.implicits._
      val source: MemoryStream[StringData] = MemoryStream[StringData](0, spark.sqlContext)
      assert(
        createAndExecuteStreamingQuery(serverData.latch, source, httpModel, processAllAvailable = true, myDf).isEmpty
      )
    }
  }

  test("Test with header and response code equals to 200 with compression") {

    withServer(dispatcher) { serverData =>
      val headers   = Map("test" -> "testHeader")
      val values    = "arrayByte"
      val myDf      = StringData(headers, values)
      val httpModel = HttpModel(
        name = "name",
        url = s"http://localhost:${serverData.port}/post-compression",
        method = "POST",
        headersFieldName = Some("headers"),
        valueFieldsNames = List("values"),
        compression = HttpCompression.Gzip,
        mediaType = "text/plain",
        logBody = false
      )
      import spark.implicits._
      val source: MemoryStream[StringData] = MemoryStream[StringData](0, spark.sqlContext)
      assert(
        createAndExecuteStreamingQuery(
          serverData.latch,
          source,
          httpModel,
          processAllAvailable = true,
          Seq.fill(1000)(myDf): _*
        ).isEmpty
      )
    }
  }

  def createAndExecuteStreamingQuery[A](
    latch: CountDownLatch,
    source: MemoryStream[A],
    httpModel: HttpModel,
    processAllAvailable: Boolean,
    myDf: A*
  ): Option[StreamingQueryException] = {

    val dsw: DataStreamWriter[Row] = new HttpWaspWriter(httpModel).write(source.toDF().repartition(10))

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

  def dispatcher(latch: CountDownLatch): Dispatcher =
    new Dispatcher {
      override def dispatch(request: RecordedRequest): MockResponse =
        request.getPath match {
          case "/post-test"        =>
            val assertion = tapPrint(
              AggregatedAssertion(
                EqualAssertion("POST", request.getMethod),
                EqualAssertion("testHeader", request.getHeader("test")),
                EqualAssertion("{\"values\":\"arrayByte\"}", request.getBody.readByteString().utf8())
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
                  "{\"values\":\"arrayByte\"}",
                  scala.util
                    .Try(new String(decompress(request.getBody.readByteArray()), StandardCharsets.UTF_8))
                    .toOption
                    .orNull
                )
              )
            )
            latch.countDown()
            assertion.toResponse
          case _                   =>
            new MockResponse().setResponseCode(404)
        }
    }
}
