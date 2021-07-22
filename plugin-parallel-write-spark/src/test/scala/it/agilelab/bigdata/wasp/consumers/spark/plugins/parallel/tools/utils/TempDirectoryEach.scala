package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools.utils

import it.agilelab.bigdata.wasp.consumers.spark.utils.SparkSuite
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.{Row, types}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.scalatest.{Args, BeforeAndAfterEach, Status, Suite}

import java.io.File
import java.nio.file.{Files, Path}
import java.util.Comparator
import collection.JavaConverters._


trait TempDirectoryEach extends BeforeAndAfterEach with SparkSuite with ParallelWriteTest {
  self: Suite =>

  val path = new File(tempDir).toPath

  override def beforeEach(): Unit = {
    super.beforeEach()
    spark.sql("CREATE TABLE IF NOT EXISTS test_table (column1 STRING, column2 STRING) STORED AS PARQUET")
    Files.createDirectories(path)
  }

  override def afterEach(): Unit = {
    spark.sql("DROP TABLE test_table")
    try {
      Files.walk(path)
        .sorted(Comparator.reverseOrder[Path]())
        .iterator()
        .asScala
        .map(_.toFile)
        .foreach(_.delete())

    } finally {
      super.afterEach()
    }
  }

  override abstract def runTest(testName: String, args: Args): Status = super[BeforeAndAfterEach].runTest(testName, args)
}

trait ContinuousUpdateTempDirectoryEach extends TempDirectoryEach {

  override def beforeEach(): Unit = {
    super.beforeEach()
    val emptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],
      StructType(List(StructField("column1", CatalystSqlParser.parseDataType("STRING")), StructField("column2", CatalystSqlParser.parseDataType("STRING")))))
    emptyDF.write.format("delta").save(path.toString)
  }
}