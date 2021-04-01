package it.agilelab.bigdata.wasp.whitelabel.models.test

import it.agilelab.bigdata.wasp.models.GenericModel
import org.mongodb.scala.bson.BsonDocument

object TestGenericModel {

  lazy val genericJson = GenericModel(
    name = "test-generic",
    kind = "parallelWrite",
    value = BsonDocument(
      """{"format": "parquet",
        |"mode": "append",
        |"partitionBy": [],
        |"requestBody": {"source":"External"}
        |}""".stripMargin)
  )

}
