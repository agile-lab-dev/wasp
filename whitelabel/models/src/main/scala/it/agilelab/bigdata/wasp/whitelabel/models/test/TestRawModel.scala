package it.agilelab.bigdata.wasp.whitelabel.models.test


import it.agilelab.bigdata.wasp.core.models.RawModel
import org.apache.spark.sql.types._

object TestRawModel {

  def apply() = RawModel(name = "TestRawModel",
    uri="hdfs://namenode:9000/user/test/",
    timed = true,
    schema = schema.json)


  private lazy val schema = StructType(Seq(
                              StructField("id", StringType),
                              StructField("number", LongType),
                              StructField("nested.field1", StringType),
                              StructField("nested.field2", LongType),
                              StructField("nested.field3", StringType)
  ))
}
