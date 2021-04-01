package it.agilelab.bigdata.wasp.master.web.openapi

import org.mongodb.scala.bson.BsonDocument

trait BsonDocumentOpenApiDefinition extends ProductOpenApi with LangOpenApi with CollectionsOpenApi {

  implicit lazy val bsonDocumentOpenApi: ToOpenApiSchema[BsonDocument] =
    objectOpenApi.mapSchema((ctx, schema) => schema.name("BsonDocument"))
}
