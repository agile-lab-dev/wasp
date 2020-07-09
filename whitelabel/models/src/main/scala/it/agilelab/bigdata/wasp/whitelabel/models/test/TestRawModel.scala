package it.agilelab.bigdata.wasp.whitelabel.models.test

import it.agilelab.bigdata.wasp.models.{RawModel, RawOptions}
import org.apache.spark.sql.types._

private[wasp] object TestRawModel {

  /* for Pipegraph */
  lazy val nested = RawModel(
    name = "TestRawNestedSchemaModel",
    uri = "hdfs://" + System.getenv("HOSTNAME") + ":9000/user/root/test_nested/",
    timed = true,
    schema = StructType(Seq(
      StructField("id", StringType),
      StructField("number", IntegerType),
      StructField("nested", StructType(Seq(
        StructField("field1", StringType),
        StructField("field2", LongType),
        StructField("field3", StringType)
      )))
    )).json)

  /* for BatchJob */
  lazy val flat = RawModel(
    name = "TestRawFlatSchemaModel",
    uri = "hdfs://" + System.getenv("HOSTNAME") + ":9000/user/root/test_flat/",
    timed = true,
    schema = StructType(Seq(
      StructField("id", StringType),
      StructField("number", LongType),
      StructField("nested.field1", StringType),
      StructField("nested.field2", LongType),
      StructField("nested.field3", StringType)
    )).json)

  lazy val text = RawModel(
    name = "TestRawTextSchemaModel",
    uri = "hdfs://" + System.getenv("HOSTNAME") + ":9000/user/root/test_flat/",
    timed = false,
    schema = StructType(Seq(
      StructField("value", StringType)
    )).json,
    options = RawOptions(
      "append",
      "text"
    )
  )
}
