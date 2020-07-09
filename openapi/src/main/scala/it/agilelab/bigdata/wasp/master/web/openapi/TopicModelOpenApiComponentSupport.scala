package it.agilelab.bigdata.wasp.master.web.openapi

import io.swagger.v3.oas.models.media.{ComposedSchema, Discriminator, Schema, StringSchema}
import it.agilelab.bigdata.wasp.models.{MultiTopicModel, TopicCompression, TopicModel}
import org.mongodb.scala.bson.BsonDocument

trait TopicModelOpenApiComponentSupport
    extends ProducerOpenApiComponentSupport
    with LangOpenApi
    with CollectionsOpenApi {

  sealed trait TopicsResponse

  implicit lazy val topicResponseOpenApi: ToOpenApiSchema[TopicsResponse] = new ToOpenApiSchema[TopicsResponse] {
    override def schema(ctx: Context): Schema[_] = {
      new ComposedSchema()
        .addOneOfItem(
          shouldBecomeARef(ctx, multiTopicModelOpenApi.schema(ctx))
        )
        .addOneOfItem(
          shouldBecomeARef(ctx, topicModelOpenApi.schema(ctx))
        )
    }
  }
  implicit lazy val multiTopicModelOpenApi: ToOpenApiSchema[MultiTopicModel] = product3(MultiTopicModel.apply)

  implicit lazy val bsonDocumentOpenApi: ToOpenApiSchema[BsonDocument] =
    objectOpenApi.mapSchema((ctx, schema) => schema.name("BsonDocument"))
  implicit lazy val topicModelOpenApi: ToOpenApiSchema[TopicModel] =
    product11(TopicModel.apply)

  implicit lazy val topicCompressionOpenApi: ToOpenApiSchema[TopicCompression] =
    new ToOpenApiSchema[TopicCompression] {
      override def schema(ctx: Context): Schema[_] = {
        val enumSchema = new StringSchema()

        TopicCompression._asString.values
          .foreach(value => enumSchema.addEnumItemObject(value))

        enumSchema.name("TopicCompressionEnum")
      }
    }
}
