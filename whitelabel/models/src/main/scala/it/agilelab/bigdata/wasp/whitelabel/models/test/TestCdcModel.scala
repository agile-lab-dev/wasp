package it.agilelab.bigdata.wasp.whitelabel.models.test

import it.agilelab.bigdata.wasp.models.{CdcModel, CdcOptions}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

private[wasp] object TestCdcModel {

  lazy val debeziumMutation = CdcModel(
    name = "TestDemeziumMutationModel",
    uri = "hdfs://" + System.getenv("HOSTNAME") + ":9000/tmp/mutations-delta-table",
    schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("first_name", StringType),
      StructField("last_name", StringType),
      StructField("email", StringType)
    )).json,
    options = CdcOptions.defaultAppend)

}
