package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools.utils

import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.utils.DataframeSchemaUtils
import it.agilelab.bigdata.wasp.consumers.spark.utils.SparkSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.{FunSuite, Suite}

class DataframeSchemaUtilsTest extends FunSuite with SparkSuite {
  self: Suite =>

  test("convertToSchema should return a dataframe compliant with the schema") {
    val sc = spark.sparkContext
    val dfSchema = StructType(StructField("b", StringType) :: StructField("c", StringType) :: StructField("a", StringType) :: Nil)
    val df = spark.createDataFrame(sc.emptyRDD[Row], dfSchema)
    val targetSchema = StructType(StructField("a", StringType) :: StructField("b", StringType) :: Nil)
    val enforcedDf = DataframeSchemaUtils.convertToSchema(df, targetSchema)
    assert(enforcedDf.isSuccess)
    assert(enforcedDf.get.columns sameElements targetSchema.names)
  }

}
