package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools.utils

import com.squareup.okhttp.mockwebserver.{Dispatcher, MockResponse, RecordedRequest}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.ParallelWriteSparkStructuredStreamingWriter
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.model.ParallelWriteModel
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery, StreamingQueryException}
import org.apache.spark.sql.{Row, SparkSession}

import java.util.concurrent.{CountDownLatch, TimeUnit}

trait ParallelWriteTest {
  protected def tempDir: String = "/tmp/tmpbucket"

  protected def writeType: String

  def dispatcher(latch: CountDownLatch): Dispatcher =
    new Dispatcher {
      override def dispatch(request: RecordedRequest): MockResponse =
        request.getPath match {
          case "/writeExecutionPlan" =>
            EqualAssertion("POST", request.getMethod)
            EqualAssertion("{\"source\":\"External\"}", request.getBody.readByteString().utf8())
            latch.countDown()


            val response: MockResponse = new MockResponse().setBody(
              s"""{
                 |    "writeUri": "${tempDir}/",
                 |    "format": "$writeType",
                 |    "writeType": "Cold",
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
                 |}""".stripMargin)
            response
          case _ =>
            new MockResponse().setResponseCode(404)
        }
    }

  def createAndExecuteStreamingQuery[A](
                                         latch: CountDownLatch,
                                         source: MemoryStream[A],
                                         genericModel: ParallelWriteModel,
                                         myDf: A*
                                       ): Option[StreamingQueryException] = {


    val dsw: DataStreamWriter[Row] = new ParallelWriteSparkStructuredStreamingWriter(genericModel).write(source.toDF().repartition(10))

    val streamingQuery: StreamingQuery = dsw.start()

    source.addData(myDf: _*)
    latch.await(1, TimeUnit.SECONDS)
    source.stop()
    streamingQuery.processAllAvailable()
    streamingQuery.stop()
    streamingQuery.awaitTermination()
    streamingQuery.exception
  }
}
