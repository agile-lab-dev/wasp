package it.agilelab.bigdata.wasp.consumers.spark.plugins.continuous.tools

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.squareup.okhttp.mockwebserver.{Dispatcher, MockResponse, RecordedRequest}
import io.delta.tables.DeltaTable
import it.agilelab.bigdata.microservicecatalog.entity.WriteExecutionPlanRequestBody
import it.agilelab.bigdata.wasp.consumers.spark.plugins.continuous.model.ContinuousUpdateModel
import it.agilelab.bigdata.wasp.consumers.spark.plugins.continuous.tools.ContinuousUpdateTestUtils.withServer
import it.agilelab.bigdata.wasp.consumers.spark.plugins.continuous.{ContinuousUpdateSparkStructuredStreamingWriter, DeltaLakeWriter}
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery, StreamingQueryException}
import org.scalatest.FunSuite
import org.scalatest.Matchers.{an, be}

case class Schema(ordering: Int, column1: String, column2: String)
case class Schema2(ordering: Int, column1: String, column2: String, column3: String)
case class Schema3(ordering1: Int, ordering2: Float, column1: String, column2: String)

object Models {
  private val entityDetails: Map[String, String] = Map(("name", "mock"))
  private val wrongDetails: Map[String, String] = Map(("key", "value"))
  private val notExistingEntityDetails: Map[String, String] = Map(("name", "entity"))

  val model1 = ContinuousUpdateModel(requestBody =  WriteExecutionPlanRequestBody(source= "External"), keys = "column1" :: Nil, tableName = "test_table", "ordering", List("ordering"), entityDetails, "localhost:4566")
  val model2 = ContinuousUpdateModel(requestBody =  WriteExecutionPlanRequestBody(source="External"), keys = "column1" :: Nil, tableName = "test_table", "-(ordering1 + ordering2)", List("ordering1", "ordering2"), entityDetails, "localhost:4566")
  val wrongModel = ContinuousUpdateModel(requestBody =  WriteExecutionPlanRequestBody(source="External"), keys = "column1" :: Nil, tableName = "test_table", "-(ordering1 + ordering2)", List("ordering1", "ordering2"), wrongDetails, "localhost:4566")
  val notExistingEntityModel = ContinuousUpdateModel(requestBody =  WriteExecutionPlanRequestBody(source="External"), keys = "column1" :: Nil, tableName = "test_table", "-(ordering1 + ordering2)", List("ordering1", "ordering2"), notExistingEntityDetails, "localhost:4566")
}

class ContinuousUpdateWaspWriterSpec extends FunSuite with TempDirectoryEach {

  test("Insert single record") {
    withServer(dispatcher) { serverData =>
      val myDf = buildData("key", 1)
      lazy val continuousUpdateModel = Models.model1

      import spark.implicits._
      val source: MemoryStream[Schema] = MemoryStream[Schema](0, spark.sqlContext)

      assert(
        createAndExecuteStreamingQuery(serverData.latch, source, continuousUpdateModel, processAllAvailable = true, myDf: _*).isEmpty
      )
      val deltaTable = DeltaTable.forPath(spark, s"s3a://$tempDir/").toDF
      assert(deltaTable.count() == 1)
      assert(deltaTable.select("column2").first().get(0) == "value1")
    }
  }

  test("Write to entity with wrong details") {
    withServer(dispatcher) { serverData =>
      val myDf = buildData("key", 1)
      lazy val continuousUpdateModel = Models.wrongModel

      import spark.implicits._
      val source: MemoryStream[Schema] = MemoryStream[Schema](0, spark.sqlContext)

      an[Exception] should be thrownBy (
        createAndExecuteStreamingQuery(serverData.latch, source, continuousUpdateModel, processAllAvailable = true, myDf: _*).isEmpty
      )
    }
  }

  test("Write to non existing entity") {
    withServer(dispatcher) { serverData =>
      val myDf = buildData("key", 1)
      lazy val continuousUpdateModel = Models.notExistingEntityModel

      import spark.implicits._
      val source: MemoryStream[Schema] = MemoryStream[Schema](0, spark.sqlContext)

      an[Exception] should be thrownBy (
        createAndExecuteStreamingQuery(serverData.latch, source, continuousUpdateModel, processAllAvailable = true, myDf: _*).isEmpty
      )
    }
  }

  test("Insert same record 1000 times") {

    withServer(dispatcher) { serverData =>
      val myDf = Schema(1, "key", "value1")
      lazy val continuousUpdateModel = Models.model1

      import spark.implicits._
      val source: MemoryStream[Schema] = MemoryStream[Schema](0, spark.sqlContext)
      assert(
        createAndExecuteStreamingQuery(serverData.latch, source, continuousUpdateModel, processAllAvailable = true, Seq.fill(1000)(myDf): _*).isEmpty
      )
      val deltaTable = DeltaTable.forPath(spark, s"s3a://$tempDir/").toDF
      assert(deltaTable.count() == 1)
      assert(deltaTable.select("column2").first().get(0) == "value1")
    }
  }

  test("Schema enforcement") {

    withServer(dispatcher) { serverData =>
      val key = "key1"
      val column2 = "testColumn2"
      val column3 = "testColumn3"
      val myDf = Schema2(1, key, column2, column3)
      lazy val continuousUpdateModel = Models.model1

      import spark.implicits._
      val source: MemoryStream[Schema2] = MemoryStream[Schema2](0, spark.sqlContext)
      an[StreamingQueryException] should be thrownBy
        createAndExecuteStreamingQuery(
          serverData.latch,
          source,
          continuousUpdateModel,
          processAllAvailable = true,
          Seq.fill(1000)(myDf): _*
        )
    }
  }

  test("Insert 2 different records") {

    withServer(dispatcher) { serverData =>
      val key1 = "key1"
      val myDf1 = Schema(1, key1, "value1")

      val key2 = "key2"
      val myDf2 = Schema(2, key2, "value2")
      lazy val continuousUpdateModel = Models.model1

      import spark.implicits._
      val source: MemoryStream[Schema] = MemoryStream[Schema](0, spark.sqlContext)
      assert(
        createAndExecuteStreamingQuery(serverData.latch, source, continuousUpdateModel, processAllAvailable = true, Seq(myDf1, myDf2): _*).isEmpty
      )
      val deltaTable = DeltaTable.forPath(spark, s"s3a://$tempDir/").toDF
      assert(deltaTable.count() == 2)
      assert(deltaTable.orderBy(col("column1")).collectAsList().get(0).get(1) == "value1")
      assert(deltaTable.orderBy(col("column1")).collectAsList().get(1).get(1) == "value2")
    }
  }

  test("Update record with key1") {
    withServer(dispatcher) { serverData =>
      val myDf1 = Schema(1, "key1", "value1")
      val myDf2 = Schema(2, "key1", "value2")
      lazy val continuousUpdateModel = Models.model1

      import spark.implicits._
      val source: MemoryStream[Schema] = MemoryStream[Schema](0, spark.sqlContext)
      assert(
        createAndExecuteStreamingQuery(serverData.latch, source, continuousUpdateModel, processAllAvailable = true, Seq.fill(1)(myDf1): _*).isEmpty
      )
      val source_2: MemoryStream[Schema] = MemoryStream[Schema](1, spark.sqlContext)
      assert(
        createAndExecuteStreamingQuery(serverData.latch, source_2, continuousUpdateModel, processAllAvailable = true, Seq.fill(1)(myDf2): _*).isEmpty
      )
      val deltaTable = DeltaTable.forPath(spark, s"s3a://$tempDir/").toDF
      assert(deltaTable.count() == 1)
      assert(deltaTable.orderBy(col("column1")).collectAsList().get(0).get(1) == "value2")
    }
  }

  test("Update record with key1, inverted ordering") {
    withServer(dispatcher) { serverData =>
      val myDf1 = Schema(2, "key1", "value1")
      val myDf2 = Schema(1, "key1", "value2")
      lazy val continuousUpdateModel = Models.model1

      import spark.implicits._
      val source: MemoryStream[Schema] = MemoryStream[Schema](0, spark.sqlContext)
      assert(
        createAndExecuteStreamingQuery(serverData.latch, source, continuousUpdateModel, processAllAvailable = true, Seq.fill(1)(myDf1): _*).isEmpty
      )
      val source_2: MemoryStream[Schema] = MemoryStream[Schema](1, spark.sqlContext)
      assert(
        createAndExecuteStreamingQuery(serverData.latch, source_2, continuousUpdateModel, processAllAvailable = true, Seq.fill(1)(myDf2): _*).isEmpty
      )
      val deltaTable = DeltaTable.forPath(spark, s"s3a://$tempDir/").toDF
      assert(deltaTable.count() == 1)
      assert(deltaTable.orderBy(col("column1")).collectAsList().get(0).get(1) == "value2")
    }
  }


  test("Data deduplication") {
    withServer(dispatcher) { serverData =>
      val data: Seq[Schema] = buildData("key", 10000)
      lazy val continuousUpdateModel = Models.model1

      import spark.implicits._
      val source: MemoryStream[Schema] = MemoryStream[Schema](0, spark.sqlContext)
      assert(
        createAndExecuteStreamingQuery(serverData.latch, source, continuousUpdateModel, processAllAvailable = true, data: _*).isEmpty
      )
      val deltaTable = DeltaTable.forPath(spark, s"s3a://$tempDir/").toDF
      assert(deltaTable.count() == 1)
      assert(deltaTable.orderBy(col("column1")).collectAsList().get(0).get(1) == "value10000")
    }
  }

  test("Data deduplication with two different keys") {
    withServer(dispatcher) { serverData =>
      val data_1: Seq[Schema] = buildData("key1", 100)
      val data: Seq[Schema] = data_1 ++ buildData("key2", 10000)
      lazy val continuousUpdateModel = Models.model1

      import spark.implicits._
      val source: MemoryStream[Schema] = MemoryStream[Schema](0, spark.sqlContext)
      assert(
        createAndExecuteStreamingQuery(serverData.latch, source, continuousUpdateModel, processAllAvailable = true, data: _*).isEmpty
      )
      val deltaTable = DeltaTable.forPath(spark, s"s3a://$tempDir/").toDF
      assert(deltaTable.count() == 2)
      assert(deltaTable.filter("column1 == 'key1'").collectAsList().get(0).get(1) == "value100")
      assert(deltaTable.filter("column1 == 'key2'").collectAsList().get(0).get(1) == "value10000")
    }
  }
  test("Data deduplication with custom ordering logic") {
    withServer(dispatcher) { serverData =>
      val data: Seq[Schema3] = Seq(
        Schema3(1, 0.5f, "key1", "value1"),
        Schema3(1, 0.7f, "key1", "value2"),
        Schema3(1, 0.01f, "key1", "value3"),
        Schema3(1, 0.3f, "key1", "value4")
      )
      lazy val continuousUpdateModel = Models.model2

      import spark.implicits._
      val source: MemoryStream[Schema3] = MemoryStream[Schema3](0, spark.sqlContext)
      assert(
        createAndExecuteStreamingQuery(serverData.latch, source, continuousUpdateModel, processAllAvailable = true, data: _*).isEmpty
      )
      val deltaTable = DeltaTable.forPath(spark, s"s3a://$tempDir/").toDF
      assert(deltaTable.count() == 1)
      assert(deltaTable.filter("column1 == 'key1'").collectAsList().get(0).get(1) == "value3")
    }
  }

  def createAndExecuteStreamingQuery[A](
                                         latch: CountDownLatch,
                                         source: MemoryStream[A],
                                         continuousUpdateModel: ContinuousUpdateModel,
                                         processAllAvailable: Boolean,
                                         myDf: A*
                                       ): Option[StreamingQueryException] = {


    val writer: DeltaLakeWriter = new DeltaLakeWriter(continuousUpdateModel, spark)

    val dsw: DataStreamWriter[Row] = new ContinuousUpdateSparkStructuredStreamingWriter(writer, continuousUpdateModel, spark).write(source.toDF().repartition(10))

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

  def buildData(key: String, n: Int): Seq[Schema] = {
    for (i <- 1 to n) yield Schema(i, key, s"value$i")
  }

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
                 |    "writeUri": "s3://${tempDir}/"
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
}
