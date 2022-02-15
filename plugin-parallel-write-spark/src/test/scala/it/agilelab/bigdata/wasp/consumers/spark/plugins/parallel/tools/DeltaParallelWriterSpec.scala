package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools

import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools.utils.DeltaTableTest
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools.utils.ParallelWriteTestUtils.withServer
import it.agilelab.bigdata.wasp.consumers.spark.utils.SparkSuite
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.scalatest.FunSuite

case class Data(column1: String, column2: String)

class DeltaParallelWriterSpec extends FunSuite with SparkSuite with DeltaTableTest {
  override def writeType = "Delta"

  test("Test output console, mode append") {

    withServer(dispatcher) { serverData =>
      val column1   = "Key"
      val column2   = "Value"
      val myDf      = Data(column1, column2)

      import spark.implicits._
      val source: MemoryStream[Data] = MemoryStream[Data](0, spark.sqlContext)
      assert(
        createAndExecuteStreamingQuery(serverData.latch, source, TestModels.model1, Seq.fill(1000)(myDf): _*).isEmpty
      )
    }
  }

  test("Test output console, mode overwrite") {

    withServer(dispatcher) { serverData =>
      val column1 = "Key"
      val column2 = "Value"
      val myDf = Data(column1, column2)
      import spark.implicits._
      val source: MemoryStream[Data] = MemoryStream[Data](0, spark.sqlContext)
      assert(
        createAndExecuteStreamingQuery(serverData.latch, source, TestModels.model2, Seq.fill(1000)(myDf): _*).isEmpty
      )
    }
  }

}
