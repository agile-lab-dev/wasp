package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools.utils

import it.agilelab.bigdata.wasp.consumers.spark.utils.SparkSuite
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.{StructField, StructType}
import org.scalatest.Suite

trait DeltaTableTest extends SparkSuite with ParallelWriteTest {
  self: Suite =>

  override def beforeEach(): Unit = {
    super.beforeEach()

    val sparkDelta = configureSparkSession(spark)

    val emptyDF = sparkDelta.createDataFrame(
      sparkDelta.sparkContext.emptyRDD[Row],
      StructType(
        List(
          StructField("column1", CatalystSqlParser.parseDataType("STRING")),
          StructField("column2", CatalystSqlParser.parseDataType("STRING"))
        )
      )
    )
    emptyDF.write.format("delta").save(tempDir)
  }

  def configureSparkSession(ss: SparkSession) = {
    val partialSSbuilder =
      SparkSession
        .builder()
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    ss.conf.getAll
      .foreach { case (k, v) => partialSSbuilder.config(k, v) }
    partialSSbuilder.getOrCreate()
  }
}
