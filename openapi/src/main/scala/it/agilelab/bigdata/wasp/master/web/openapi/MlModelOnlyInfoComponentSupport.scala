package it.agilelab.bigdata.wasp.master.web.openapi

import it.agilelab.bigdata.wasp.core.models.MlModelOnlyInfo
import org.mongodb.scala.bson.BsonObjectId

trait MlModelOnlyInfoComponentSupport
    extends LangOpenApi
    with ProductOpenApi
    with CollectionsOpenApi {
  implicit lazy val bsonObjectIdOpenApi: ToOpenApiSchema[BsonObjectId] =
    stringOpenApi.substituteOf[BsonObjectId]

  implicit lazy val mlMOdelOnlyInfoOpenApi: ToOpenApiSchema[MlModelOnlyInfo] =
    product8(MlModelOnlyInfo)
}
