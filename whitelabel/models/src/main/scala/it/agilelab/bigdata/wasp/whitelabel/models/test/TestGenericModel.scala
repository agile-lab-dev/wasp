package it.agilelab.bigdata.wasp.whitelabel.models.test

import it.agilelab.bigdata.wasp.models.GenericModel
import org.mongodb.scala.bson.BsonDocument

object TestGenericModel {

  lazy val parallelWriteModel = GenericModel(
    name = "test-generic",
    kind = "parallelWrite",
    value = BsonDocument(
      """{"format": "parquet",
        |"mode": "append",
        |"partitionBy": [],
        |"requestBody": {"source":"External"}
        |"entityDetails":{"name": "integrationTest"},
        |"s3aEndpoint": "host.docker.internal:4566"
        |}""".stripMargin)
  )

  lazy val continuousUpdateModel = GenericModel(
    name = "test-continuous-update",
    kind = "continuousUpdate",
    value = BsonDocument(
      """{"requestBody": {"source":"External"},
        |"keys": ["id"],
        |"tableName": "topic_table",
        |"orderingExpression": "number",
        |"fieldsToDrop": []
        |"entityDetails": {"name": "integrationTest"}
        |"s3aEndpoint": "host.docker.internal:4566"
        |}""".stripMargin)
  )

}
