package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools

import com.squareup.okhttp.mockwebserver.{Dispatcher, MockResponse, RecordedRequest}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.ParallelWriteSparkStructuredStreamingWriter
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools.ParallelWriteTestUtils.{tapPrint, withServer}
import it.agilelab.bigdata.wasp.consumers.spark.utils.SparkSuite
import it.agilelab.bigdata.wasp.models.GenericModel
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery, StreamingQueryException}
import org.mongodb.scala.bson.BsonDocument
import org.scalatest.FunSuite
import org.scalatest.Matchers.{an, be, convertToAnyShouldWrapper}

import java.util.concurrent.{CountDownLatch, TimeUnit}

case class ByteData(headers: Map[String, String], values: Array[Byte])
case class StringData(headers: Map[String, String], values: String)


class ParallelWriteWaspWriterSpec extends FunSuite with SparkSuite {

  test("Test output console, mode append") {

    withServer(dispatcher) { serverData =>
      val column1   = Map("test" -> "testColumn1")
      val column2    = "arrayByte"
      val myDf      = StringData(column1, column2)
      lazy val genericModel = GenericModel(
        name = "test-generic",
        kind = "parallelWrite",
        value = BsonDocument(
          """{"format": "console",
            |"mode": "append",
            |"partitionBy": [],
            |"requestBody": {"source":"External"}
            |}""".stripMargin)
      )
      import spark.implicits._
      val source: MemoryStream[StringData] = MemoryStream[StringData](0, spark.sqlContext)
      assert(
        createAndExecuteStreamingQuery(serverData.latch, source, genericModel, processAllAvailable = true, Seq.fill(1000)(myDf): _*).isEmpty
      )
    }
  }

  test("Test output console, mode overwrite") {

    withServer(dispatcher) { serverData =>
      val column1   = Map("test" -> "testColumn1")
      val column2    = "arrayByte"
      val myDf      = StringData(column1, column2)
      lazy val genericModel = GenericModel(
        name = "test-generic",
        kind = "parallelWrite",
        value = BsonDocument(
          """{"format": "console",
            |"mode": "append",
            |"partitionBy": [],
            |"requestBody": {"source":"External"}
            |}""".stripMargin)
      )
      import spark.implicits._
      val source: MemoryStream[StringData] = MemoryStream[StringData](0, spark.sqlContext)
      assert(
        createAndExecuteStreamingQuery(serverData.latch, source, genericModel, processAllAvailable = true, Seq.fill(1000)(myDf): _*).isEmpty
      )
    }
  }




  test("""Test with kind not "parallelWrite"""") {

    withServer(dispatcher) { serverData =>
      val column1   = Map("test" -> "testColumn1")
      val column2    = "arrayByte"
      val myDf      = StringData(column1, column2)
      lazy val genericModel = GenericModel(
        name = "test-generic",
        kind = "generic",
            value = BsonDocument(
              """{"format": "console",
                |"mode": "append",
                |"partitionBy": [],
                |"requestBody": {"source":"External"}
                |}""".stripMargin)
      )

      import spark.implicits._
      val source: MemoryStream[StringData] = MemoryStream[StringData](0, spark.sqlContext)

      an [IllegalArgumentException] should be thrownBy
        createAndExecuteStreamingQuery(
          serverData.latch,
          source,
          genericModel,
          processAllAvailable = true,
          Seq.fill(1000)(myDf): _*
        )

    }
  }

  def createAndExecuteStreamingQuery[A](
                                         latch: CountDownLatch,
                                         source: MemoryStream[A],
                                         genericModel: GenericModel,
                                         processAllAvailable: Boolean,
                                         myDf: A*
  ): Option[StreamingQueryException] = {

    val dsw: DataStreamWriter[Row] = new ParallelWriteSparkStructuredStreamingWriter(genericModel, spark).write(source.toDF().repartition(10))

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
          case "/writeExecutionPlan" =>
            val assertion = tapPrint(
              AggregatedAssertion(
                EqualAssertion("POST", request.getMethod),
                EqualAssertion("{\"source\":\"External\"}", request.getBody.readByteString().utf8())
              )
            )
            latch.countDown()
            val response: MockResponse = new MockResponse().setBody(
              """{
                |    "temporaryCredentials": {
                |        "r": {
                |            "accessKeyID": "ReadaccessKeyID",
                |            "secretKey": "ReadsecretKey",
                |            "sessionToken": "ReadsessionToken"
                |        },
                |        "w": {
                |            "accessKeyID": "WriteaccessKeyID",
                |            "secretKey": "WritesecretKey",
                |            "sessionToken": "WritesessionToken"
                |        }
                |    },
                |    "writeType": "Cold",
                |    "writeUri": "s3://mytestbucket/test3/"
                |}""".stripMargin)
            response
          case _                   =>
            new MockResponse().setResponseCode(404)
        }
    }
}
