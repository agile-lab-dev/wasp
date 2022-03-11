package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools

import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools.utils.ParallelWriteTest
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools.utils.ParallelWriteTestUtils.withServer
import it.agilelab.bigdata.wasp.consumers.spark.utils.SparkSuite
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.types.{ MapType, StringType, StructField, StructType }
import org.scalatest.FunSuite

case class ByteData(headers: Map[String, String], values: Array[Byte])
case class StringData(headers: Map[String, String], values: String)

class ParallelWriterSpec extends FunSuite with SparkSuite with ParallelWriteTest {
  override def writeType = "Parquet"

  private val tableSchema = StructType(
    StructField("headers", MapType(StringType, StringType)) :: StructField("values", StringType) :: Nil
  )

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
