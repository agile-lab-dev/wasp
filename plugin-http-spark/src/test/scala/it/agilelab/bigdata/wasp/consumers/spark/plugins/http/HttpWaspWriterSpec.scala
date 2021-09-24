package it.agilelab.bigdata.wasp.consumers.spark.plugins.http

import com.squareup.okhttp.mockwebserver.{Dispatcher, MockResponse, RecordedRequest}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.http.HttpTestUtils.{decompress, tapPrint, withServer}
import it.agilelab.bigdata.wasp.consumers.spark.utils.SparkSuite
import it.agilelab.bigdata.wasp.models.{HttpCompression, HttpModel}
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery, StreamingQueryException}
import org.scalatest.FunSuite

import java.nio.charset.StandardCharsets
import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.collection.JavaConverters._

class HttpWaspWriterSpec extends FunSuite with SparkSuite {
  test("Test with header, StringType value and response code equals to 200") {

    withServer(dispatcher) { serverData =>
      val headers = Map("test" -> "testHeader", "dataType" -> "StringData")
      val values  = "arrayByte"
      val myDf    = StringData(headers, values)
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

  test("Test with header, ArrayType value, httpModel unstructured and response code equals to 200") {

    withServer(dispatcher) { serverData =>
      val httpModel = HttpModel(
        name = "name",
        url = s"http://localhost:${serverData.port}/post-test",
        method = "POST",
        headersFieldName = Some("headers"),
        valueFieldsNames = List("values"),
        compression = HttpCompression.Disabled,
        mediaType = "text/plain",
        logBody = true,
        structured = false
      )
      val headers =
        Map("test" -> "testHeader", "dataType" -> "ArrayData", "httpModelStructured" -> httpModel.structured.toString)
      val values = Array("val1", "val2")
      val myDf   = ArrayData(headers, values)
      import spark.implicits._
      val source: MemoryStream[ArrayData] = MemoryStream[ArrayData](0, spark.sqlContext)
      assert(
        createAndExecuteStreamingQuery(serverData.latch, source, httpModel, processAllAvailable = true, myDf).isEmpty
      )
    }
  }

  test("Test with header, ArrayType value, httpModel structured and response code equals to 200") {

    withServer(dispatcher) { serverData =>
      val httpModel = HttpModel(
        name = "name",
        url = s"http://localhost:${serverData.port}/post-test",
        method = "POST",
        headersFieldName = Some("headers"),
        valueFieldsNames = List("values"),
        compression = HttpCompression.Disabled,
        mediaType = "text/plain",
        logBody = true,
        structured = true
      )
      val headers =
        Map("test" -> "testHeader", "dataType" -> "ArrayData", "httpModelStructured" -> httpModel.structured.toString)
      val values = Array("val1", "val2")
      val myDf   = ArrayData(headers, values)
      import spark.implicits._
      val source: MemoryStream[ArrayData] = MemoryStream[ArrayData](0, spark.sqlContext)
      assert(
        createAndExecuteStreamingQuery(serverData.latch, source, httpModel, processAllAvailable = true, myDf).isEmpty
      )
    }
  }

  test("Test with header, MapType value, and response code equals to 200") {

    withServer(dispatcher) { serverData =>
      val headers = Map("test" -> "testHeader", "dataType" -> "MapData")
      val values  = Map("val1" -> "val2")
      val myDf    = MapData(headers, values)
      val httpModel = HttpModel(
        name = "name",
        url = s"http://localhost:${serverData.port}/post-test",
        method = "POST",
        headersFieldName = Some("headers"),
        valueFieldsNames = List("values"),
        compression = HttpCompression.Disabled,
        mediaType = "text/plain",
        logBody = true,
        structured = false
      )
      import spark.implicits._
      val source: MemoryStream[MapData] = MemoryStream[MapData](0, spark.sqlContext)
      assert(
        createAndExecuteStreamingQuery(serverData.latch, source, httpModel, processAllAvailable = true, myDf).isEmpty
      )
    }
  }

  test("Test with header, StructType value, and response code equals to 200") {

    withServer(dispatcher) { serverData =>
      val headers = Map("test" -> "testHeader", "dataType" -> "StructData")
      val values  = Struct("key", "val1")
      val myDf    = StructData(headers, values)
      val httpModel = HttpModel(
        name = "name",
        url = s"http://localhost:${serverData.port}/post-test",
        method = "POST",
        headersFieldName = Some("headers"),
        valueFieldsNames = List("values"),
        compression = HttpCompression.Disabled,
        mediaType = "text/plain",
        logBody = true
      )
      import spark.implicits._
      val source: MemoryStream[StructData] = MemoryStream[StructData](0, spark.sqlContext)
      assert(
        createAndExecuteStreamingQuery(serverData.latch, source, httpModel, processAllAvailable = true, myDf).isEmpty
      )
    }
  }

  test("Test with header, multiple values, and response code equals to 200") {

    withServer(dispatcher) { serverData =>
      val headers    = Map("test" -> "testHeader", "dataType" -> "MultipleStringData")
      val values     = "val1"
      val moreValues = "val2"
      val myDf       = MultipleStringData(headers, values, moreValues)
      val httpModel = HttpModel(
        name = "name",
        url = s"http://localhost:${serverData.port}/post-test",
        method = "POST",
        headersFieldName = Some("headers"),
        valueFieldsNames = List("values", "moreValues"),
        compression = HttpCompression.Disabled,
        mediaType = "text/plain",
        logBody = true
      )
      import spark.implicits._
      val source: MemoryStream[MultipleStringData] = MemoryStream[MultipleStringData](0, spark.sqlContext)
      assert(
        createAndExecuteStreamingQuery(serverData.latch, source, httpModel, processAllAvailable = true, myDf).isEmpty
      )
    }
  }

  test("Test with header and response code equals to 200 with compression") {

    withServer(dispatcher) { serverData =>
      val headers = Map("test" -> "testHeader")
      val values  = "arrayByte"
      val myDf    = StringData(headers, values)
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
          case "/post-test" =>
            val assertion = tapPrint(
              AggregatedAssertion(
                EqualAssertion("POST", request.getMethod),
                EqualAssertion("testHeader", request.getHeader("test")), {
                  val body = request.getBody.readByteString().utf8().trim
                  request.getHeader("dataType") match {
                    case "StringData" => EqualAssertion("{\"values\":\"arrayByte\"}", body)
                    case "ArrayData" =>
                      if (request.getHeader("httpModelStructured").toBoolean)
                        EqualAssertion("{\"values\":[\"val1\",\"val2\"]}", body)
                      else EqualAssertion("[\"val1\",\"val2\"]", body)
                    case "MapData"            => EqualAssertion("{\"val1\":\"val2\"}", body)
                    case "StructData"         => EqualAssertion("{\"name\":\"key\",\"value\":\"val1\"}", body)
                    case "MultipleStringData" => EqualAssertion("{\"values\":\"val1\",\"moreValues\":\"val2\"}", body)
                  }
                }
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
          case _ =>
            new MockResponse().setResponseCode(404)
        }
    }

  test("prepareDF passing an unsupported datatype as header column") {
    val httpModel = HttpModel(
      name = "name",
      url = s"http://localhost/post-test",
      method = "POST",
      headersFieldName = Some("headers"),
      valueFieldsNames = List("values"),
      compression = HttpCompression.Disabled,
      mediaType = "text/plain",
      logBody = true,
      structured = true
    )
    import spark.implicits._
    val target = new HttpWaspWriter(httpModel)
    intercept[RuntimeException] {
      target.prepareDF(
        spark
          .createDataset(
            Seq(3 -> Array((1, 2, 3), (1, 3, 4), (4, 5, 4)))
          )
          .toDF()
          .select(col("_1").as("values"), col("_2").as("headers"))
      )
    }
  }

  test("prepareDF passing an array of struct as header column") {
    val httpModel = HttpModel(
      name = "name",
      url = s"http://localhost/post-test",
      method = "POST",
      headersFieldName = Some("headers"),
      valueFieldsNames = List("values"),
      compression = HttpCompression.Disabled,
      mediaType = "text/plain",
      logBody = true,
      structured = true
    )
    import spark.implicits._
    val target = new HttpWaspWriter(httpModel)
    val result = target
      .prepareDF(
        spark
          .createDataset(
            Seq(3 -> Array(KafkaMetadataHeader("k", "v".getBytes(StandardCharsets.UTF_8))))
          )
          .toDF()
          .select(col("_1").as("values"), col("_2").as("headers"))
      )
      .first()
    assert(result.getJavaMap[String, String](0).asScala === Map("k" -> "v"))
    assert(new String(result.getAs[Array[Byte]](1), StandardCharsets.UTF_8) === """{"values":3}""")
  }

  test("prepareDF passing a map with wrong inner types") {
    val httpModel = HttpModel(
      name = "name",
      url = s"http://localhost/post-test",
      method = "POST",
      headersFieldName = Some("headers"),
      valueFieldsNames = List("values"),
      compression = HttpCompression.Disabled,
      mediaType = "text/plain",
      logBody = true,
      structured = true
    )
    import spark.implicits._
    val target = new HttpWaspWriter(httpModel)
    val result = target
      .prepareDF(
        spark
          .createDataset(
            Seq(3 -> Map(3 -> 4, 7 -> 8))
          )
          .toDF()
          .select(col("_1").as("values"), col("_2").as("headers"))
      )
      .first()
    assert(result.getJavaMap[String, String](0).asScala === Map("3" -> "4", "7" -> "8"))
    assert(new String(result.getAs[Array[Byte]](1), StandardCharsets.UTF_8) === """{"values":3}""")
  }
}

case class KafkaMetadataHeader(headerKey: String, headerValue: Array[Byte])
