package it.agilelab.bigdata.wasp.whitelabel.models.test

import it.agilelab.bigdata.wasp.core.models.DocumentModel
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

object TestMongoModel {

  lazy val writeToMongo = DocumentModel(
    name = "test-write-to-mongo",
    connectionString = ConfigManager.getMongoDBConfig.address + "/" +  ConfigManager.getMongoDBConfig.databaseName + "." + "TestCollectionStructuredWriteMongo",
    schema = StructType(Seq(
      StructField("id", StringType),
      StructField("number", IntegerType),
      StructField("nested", StructType(Seq(
        StructField("field1", StringType),
        StructField("field2", LongType),
        StructField("field3", StringType)
      )))
    )).json
  )

}
