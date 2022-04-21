package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools

import com.squareup.okhttp.mockwebserver.{Dispatcher, MockResponse, RecordedRequest}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools.utils.{EqualAssertion, ParallelWriteTest}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools.utils.ParallelWriteTestUtils.withServer
import it.agilelab.bigdata.wasp.consumers.spark.utils.SparkSuite
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.FunSuite

import java.util.concurrent.CountDownLatch

class HotParallelWriterSpec extends FunSuite with SparkSuite with ParallelWriteTest{
  override protected def writeType: String = "Hot"

  private val tableSchema = StructType(
    StructField("headers", StringType) :: StructField("values", StringType) :: Nil
  )

  override def dispatcher(latch: CountDownLatch): Dispatcher =
    new Dispatcher {
      override def dispatch(request: RecordedRequest): MockResponse = {
        request.getPath match {
          case "/data/committed" =>
            EqualAssertion("GET", request.getMethod)
            EqualAssertion(false, request.getHeader("X-Plt-Correlation-ID").isEmpty)
            val response: MockResponse = new MockResponse().setBody(s"""{
                                                                       |    "commitStatus": "Success"
                                                                       |}""".stripMargin)
            response
          case "/data/complete" =>
            EqualAssertion("POST", request.getMethod)
            EqualAssertion(false, request.getHeader("X-Plt-Correlation-ID").isEmpty)
            val response: MockResponse = new MockResponse().setBody(s"""{}""".stripMargin)
            response
          case "/data/stream" =>
            EqualAssertion("POST", request.getMethod)
            EqualAssertion(false, request.getHeader("X-Plt-Correlation-ID").isEmpty)
            val response: MockResponse = new MockResponse().setBody(s"""{}""".stripMargin)
            response
          case "/writeExecutionPlan" =>
            EqualAssertion("POST", request.getMethod)
            EqualAssertion(false, request.getHeader("X-Plt-Correlation-ID").isEmpty)
            latch.countDown()

            val response: MockResponse = new MockResponse().setBody(s"""{
                                                                       |    "writeType": "Hot"
                                                                       |}""".stripMargin)
            response
          case _ =>
            new MockResponse().setResponseCode(404)
        }
      }
    }

  test("Test output console, mode append") {

    withServer(dispatcher) { serverData =>
      val column1 = Map("test" -> "testColumn1")
      val column2 = "arrayByte"
      val myDf    = StringData(column1, column2)

      import spark.implicits._
      val source: MemoryStream[StringData] = MemoryStream[StringData](0, spark.sqlContext)
      assert(
        createAndExecuteStreamingQuery(
          serverData.latch,
          source,
          TestModels.model1,
          tableSchema,
          Seq.fill(1000)(myDf): _*
        ).isEmpty
      )
    }
  }

  test("Test output console, mode overwrite") {

    withServer(dispatcher) { serverData =>
      val column1 = Map("test" -> "testColumn1")
      val column2 = "arrayByte"
      val myDf    = StringData(column1, column2)
      import spark.implicits._
      val source: MemoryStream[StringData] = MemoryStream[StringData](0, spark.sqlContext)
      assert(
        createAndExecuteStreamingQuery(
          serverData.latch,
          source,
          TestModels.model2,
          tableSchema,
          Seq.fill(1000)(myDf): _*
        ).isEmpty
      )
    }
  }
}
