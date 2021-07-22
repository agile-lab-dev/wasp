package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools

import io.delta.tables.DeltaTable
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.model.{ContinuousUpdate, ParallelWriteModel}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools.utils.ParallelWriteTestUtils.withServer
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools.utils.{ContinuousUpdateTempDirectoryEach, TempDirectoryEach}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.StreamingQueryException
import org.scalatest.FunSuite
import org.scalatest.Matchers.{an, be}

case class Schema(ordering: Int, column1: String, column2: String)
case class Schema2(ordering: Int, column1: String, column2: String, column3: String)
case class Schema3(ordering1: Int, ordering2: Float, column1: String, column2: String)

object ContinuousUpdateModels {
  private val entityDetails: Map[String, String] = Map(("name", "mock"))
  private val wrongDetails: Map[String, String] = Map(("key", "value"))
  private val notExistingEntityDetails: Map[String, String] = Map(("name", "entity"))

  val model1 = ParallelWriteModel(ContinuousUpdate(
    tableName = "test_table",
    keys = "column1" :: Nil,
    orderingExpression = "ordering",
    fieldsToDrop = List("ordering")
  ), entityDetails)

  val model2 = ParallelWriteModel(ContinuousUpdate(
    tableName = "test_table",
    keys = "column1" :: Nil,
    orderingExpression = "-(ordering1 + ordering2)",
    fieldsToDrop = List("ordering1", "ordering2")
  ), entityDetails)

  val wrongModel = ParallelWriteModel(ContinuousUpdate(
    tableName = "test_table",
    keys = "column1" :: Nil,
    orderingExpression = "-(ordering1 + ordering2)",
    fieldsToDrop = List("ordering1", "ordering2")
  ), wrongDetails)

  val notExistingEntityModel = ParallelWriteModel(ContinuousUpdate(
    tableName = "test_table",
    keys = "column1" :: Nil,
    orderingExpression = "-(ordering1 + ordering2)",
    fieldsToDrop = List("ordering1", "ordering2")
  ), notExistingEntityDetails)
}

class ContinuousUpdateWriterSpec extends FunSuite with ContinuousUpdateTempDirectoryEach {
  override def writeType = "Delta"

  test("Insert single record") {
    withServer(dispatcher) { serverData =>
      val myDf = buildData("key", 1)
      lazy val continuousUpdateModel = ContinuousUpdateModels.model1

      import spark.implicits._
      val source: MemoryStream[Schema] = MemoryStream[Schema](0, spark.sqlContext)

      assert(
        createAndExecuteStreamingQuery(serverData.latch, source, continuousUpdateModel, myDf: _*).isEmpty
      )
      val deltaTable = DeltaTable.forPath(spark, tempDir).toDF
      assert(deltaTable.count() == 1)
      assert(deltaTable.select("column2").first().get(0) == "value1")
    }
  }

  test("Write to entity with wrong details") {
    withServer(dispatcher) { serverData =>
      val myDf = buildData("key", 1)
      lazy val continuousUpdateModel = ContinuousUpdateModels.wrongModel

      import spark.implicits._
      val source: MemoryStream[Schema] = MemoryStream[Schema](0, spark.sqlContext)

      an[Exception] should be thrownBy (
        createAndExecuteStreamingQuery(serverData.latch, source, continuousUpdateModel, myDf: _*).isEmpty
      )
    }
  }

  test("Write to non existing entity") {
    withServer(dispatcher) { serverData =>
      val myDf = buildData("key", 1)
      lazy val continuousUpdateModel = ContinuousUpdateModels.notExistingEntityModel

      import spark.implicits._
      val source: MemoryStream[Schema] = MemoryStream[Schema](0, spark.sqlContext)

      an[Exception] should be thrownBy (
        createAndExecuteStreamingQuery(serverData.latch, source, continuousUpdateModel, myDf: _*).isEmpty
      )
    }
  }

  test("Insert same record 1000 times") {

    withServer(dispatcher) { serverData =>
      val myDf = Schema(1, "key", "value1")
      lazy val continuousUpdateModel = ContinuousUpdateModels.model1

      import spark.implicits._
      val source: MemoryStream[Schema] = MemoryStream[Schema](0, spark.sqlContext)
      assert(
        createAndExecuteStreamingQuery(serverData.latch, source, continuousUpdateModel, Seq.fill(1000)(myDf): _*).isEmpty
      )
      val deltaTable = DeltaTable.forPath(spark, tempDir).toDF
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
      lazy val continuousUpdateModel = ContinuousUpdateModels.model1

      import spark.implicits._
      val source: MemoryStream[Schema2] = MemoryStream[Schema2](0, spark.sqlContext)
      an[StreamingQueryException] should be thrownBy
        createAndExecuteStreamingQuery(
          serverData.latch,
          source,
          continuousUpdateModel,
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
      lazy val continuousUpdateModel = ContinuousUpdateModels.model1

      import spark.implicits._
      val source: MemoryStream[Schema] = MemoryStream[Schema](0, spark.sqlContext)
      assert(
        createAndExecuteStreamingQuery(serverData.latch, source, continuousUpdateModel, Seq(myDf1, myDf2): _*).isEmpty
      )
      val deltaTable = DeltaTable.forPath(spark, tempDir).toDF
      assert(deltaTable.count() == 2)
      assert(deltaTable.orderBy(col("column1")).collectAsList().get(0).get(1) == "value1")
      assert(deltaTable.orderBy(col("column1")).collectAsList().get(1).get(1) == "value2")
    }
  }

  test("Update record with key1") {
    withServer(dispatcher) { serverData =>
      val myDf1 = Schema(1, "key1", "value1")
      val myDf2 = Schema(2, "key1", "value2")
      lazy val continuousUpdateModel = ContinuousUpdateModels.model1

      import spark.implicits._
      val source: MemoryStream[Schema] = MemoryStream[Schema](0, spark.sqlContext)
      assert(
        createAndExecuteStreamingQuery(serverData.latch, source, continuousUpdateModel, Seq.fill(1)(myDf1): _*).isEmpty
      )
      val source_2: MemoryStream[Schema] = MemoryStream[Schema](1, spark.sqlContext)
      assert(
        createAndExecuteStreamingQuery(serverData.latch, source_2, continuousUpdateModel, Seq.fill(1)(myDf2): _*).isEmpty
      )
      val deltaTable = DeltaTable.forPath(spark, tempDir).toDF
      assert(deltaTable.count() == 1)
      assert(deltaTable.orderBy(col("column1")).collectAsList().get(0).get(1) == "value2")
    }
  }

  test("Update record with key1, inverted ordering") {
    withServer(dispatcher) { serverData =>
      val myDf1 = Schema(2, "key1", "value1")
      val myDf2 = Schema(1, "key1", "value2")
      lazy val continuousUpdateModel = ContinuousUpdateModels.model1

      import spark.implicits._
      val source: MemoryStream[Schema] = MemoryStream[Schema](0, spark.sqlContext)
      assert(
        createAndExecuteStreamingQuery(serverData.latch, source, continuousUpdateModel, Seq.fill(1)(myDf1): _*).isEmpty
      )
      val source_2: MemoryStream[Schema] = MemoryStream[Schema](1, spark.sqlContext)
      assert(
        createAndExecuteStreamingQuery(serverData.latch, source_2, continuousUpdateModel, Seq.fill(1)(myDf2): _*).isEmpty
      )
      val deltaTable = DeltaTable.forPath(spark, tempDir).toDF
      assert(deltaTable.count() == 1)
      assert(deltaTable.orderBy(col("column1")).collectAsList().get(0).get(1) == "value2")
    }
  }


  test("Data deduplication") {
    withServer(dispatcher) { serverData =>
      val data: Seq[Schema] = buildData("key", 10000)
      lazy val continuousUpdateModel = ContinuousUpdateModels.model1

      import spark.implicits._
      val source: MemoryStream[Schema] = MemoryStream[Schema](0, spark.sqlContext)
      assert(
        createAndExecuteStreamingQuery(serverData.latch, source, continuousUpdateModel, data: _*).isEmpty
      )
      val deltaTable = DeltaTable.forPath(spark, tempDir).toDF
      assert(deltaTable.count() == 1)
      assert(deltaTable.orderBy(col("column1")).collectAsList().get(0).get(1) == "value10000")
    }
  }

  test("Data deduplication with two different keys") {
    withServer(dispatcher) { serverData =>
      val data_1: Seq[Schema] = buildData("key1", 100)
      val data: Seq[Schema] = data_1 ++ buildData("key2", 10000)
      lazy val continuousUpdateModel = ContinuousUpdateModels.model1

      import spark.implicits._
      val source: MemoryStream[Schema] = MemoryStream[Schema](0, spark.sqlContext)
      assert(
        createAndExecuteStreamingQuery(serverData.latch, source, continuousUpdateModel, data: _*).isEmpty
      )
      val deltaTable = DeltaTable.forPath(spark, tempDir).toDF
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
      lazy val continuousUpdateModel = ContinuousUpdateModels.model2

      import spark.implicits._
      val source: MemoryStream[Schema3] = MemoryStream[Schema3](0, spark.sqlContext)
      assert(
        createAndExecuteStreamingQuery(serverData.latch, source, continuousUpdateModel, data: _*).isEmpty
      )
      val deltaTable = DeltaTable.forPath(spark, tempDir).toDF
      assert(deltaTable.count() == 1)
      assert(deltaTable.filter("column1 == 'key1'").collectAsList().get(0).get(1) == "value3")
    }
  }



  def buildData(key: String, n: Int): Seq[Schema] = {
    for (i <- 1 to n) yield Schema(i, key, s"value$i")
  }


}
