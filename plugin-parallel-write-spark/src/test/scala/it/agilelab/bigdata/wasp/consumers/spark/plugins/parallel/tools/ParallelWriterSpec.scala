package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools

import it.agilelab.bigdata.microservicecatalog.entity.WriteExecutionPlanRequestBody
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.model.{ParallelWrite, ParallelWriteModel}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools.utils.ParallelWriteTestUtils.withServer
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools.utils.TempDirectoryEach
import it.agilelab.bigdata.wasp.consumers.spark.utils.SparkSuite
import it.agilelab.bigdata.wasp.models.GenericModel
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.mongodb.scala.bson.BsonDocument
import org.scalatest.FunSuite

case class ByteData(headers: Map[String, String], values: Array[Byte])
case class StringData(headers: Map[String, String], values: String)


object Models {
  val model1 = ParallelWriteModel(ParallelWrite("append", partitionBy = Some(List.empty)), entityDetails = Map(("name", "mock")))
  val model2 = ParallelWriteModel(ParallelWrite("overwrite", partitionBy = Some(List.empty)), entityDetails = Map(("name", "mock")))
}

class ParallelWriterSpec extends FunSuite with SparkSuite with TempDirectoryEach {
  override def writeType = "Parquet"

  test("Test output console, mode append") {

    withServer(dispatcher) { serverData =>
      val column1   = Map("test" -> "testColumn1")
      val column2    = "arrayByte"
      val myDf      = StringData(column1, column2)

      import spark.implicits._
      val source: MemoryStream[StringData] = MemoryStream[StringData](0, spark.sqlContext)
      assert(
        createAndExecuteStreamingQuery(serverData.latch, source, Models.model1, Seq.fill(1000)(myDf): _*).isEmpty
      )
    }
  }

  test("Test output console, mode overwrite") {

    withServer(dispatcher) { serverData =>
      val column1 = Map("test" -> "testColumn1")
      val column2 = "arrayByte"
      val myDf = StringData(column1, column2)
      import spark.implicits._
      val source: MemoryStream[StringData] = MemoryStream[StringData](0, spark.sqlContext)
      assert(
        createAndExecuteStreamingQuery(serverData.latch, source, Models.model2, Seq.fill(1000)(myDf): _*).isEmpty
      )
    }
  }

//  test("""Test with kind not "parallelWrite"""") {
//
//    withServer(dispatcher) { serverData =>
//      val column1   = Map("test" -> "testColumn1")
//      val column2    = "arrayByte"
//      val myDf      = StringData(column1, column2)
//      lazy val genericModel = GenericModel(
//        name = "test-generic",
//        kind = "generic",
//            value = BsonDocument(
//              """{"format": "console",
//                |"mode": "append",
//                |"partitionBy": [],
//                |"entityDetails": {"name":"mock"},
//                |"s3aEndpoint": "localhost:4566"
//                |}""".stripMargin)
//      )
//
//      import spark.implicits._
//      val source: MemoryStream[StringData] = MemoryStream[StringData](0, spark.sqlContext)
//
//      an [IllegalArgumentException] should be thrownBy
//        createAndExecuteStreamingQuery(
//          serverData.latch,
//          source,
//          genericModel,
//          processAllAvailable = true,
//          Seq.fill(1000)(myDf): _*
//        )
//
//    }
//  }


}
