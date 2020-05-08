package it.agilelab.bigdata.wasp.master.web.openapi

import it.agilelab.bigdata.wasp.core.models.MlModelOnlyInfo
import org.mongodb.scala.bson.BsonObjectId

trait MlModelOnlyInfoComponentSupport extends LangOpenApi with ProductOpenApi with CollectionsOpenApi {
  implicit lazy val bsonObjectIdOpenApi: ToOpenApiSchema[BsonObjectId] =
    stringOpenApi.substituteOf[BsonObjectId]

  implicit lazy val mlMOdelOnlyInfoOpenApi: ToOpenApiSchema[MlModelOnlyInfo] =
    product7(MlModelOnlyInfo).mapSchema { (_, schema) =>
      schema.getProperties
        .get("modelFileId")
        .example("507f1f77bcf86cd799439011")
        .description("Should be a valid mongodb bsonobject formatted as hex string")
      schema
    }
}
