package it.agilelab.bigdata.wasp.whitelabel.models.test

import it.agilelab.bigdata.wasp.datastores.GenericProduct
import it.agilelab.bigdata.wasp.models.GenericModel
import org.mongodb.scala.bson.BsonDocument

object TestParallelWriteModel {

  lazy val parallelWriteModel = GenericModel(
    name = "test-parquet-parallelwrite",
    value = BsonDocument(
      """{"writerDetails":{
        | "writerType": "parallelWrite",
        | "saveMode": "append",
        | "partitionBy": []
        |}
        |"entityDetails":{"name": "integrationTest"},
        |}""".stripMargin),
    product = GenericProduct("parallelWrite", None)
  )

  lazy val continuousUpdateModel = GenericModel(
    name = "test-delta-parallelwrite",
    value = BsonDocument(
      """{
        | "entityDetails": {"name": "integrationTest"},
        | "writerDetails": {
        |   "writerType": "continuousUpdate",
        |   "keys": ["id"],
        |   "tableName": "topic_table",
        |   "orderingExpression": "number",
        |   "fieldsToDrop": []
        | }
        |}""".stripMargin),
    product = GenericProduct("parallelWrite", None)
  )

}
