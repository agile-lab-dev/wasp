package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools.utils

import it.agilelab.bigdata.wasp.consumers.spark.utils.SparkSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.{StructField, StructType}
import org.scalatest.Suite


trait DeltaTableTest extends SparkSuite with ParallelWriteTest {
  self: Suite =>

  override def beforeEach(): Unit = {
    super.beforeEach()
    val emptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],
      StructType(List(StructField("column1", CatalystSqlParser.parseDataType("STRING")), StructField("column2", CatalystSqlParser.parseDataType("STRING")))))
    emptyDF.write.format("delta").save(tempDir)
  }

}
