package it.agilelab.bigdata.wasp.repository.mongo.providers

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import it.agilelab.bigdata.wasp.models.{JobStatus}
import it.agilelab.bigdata.wasp.repository.core.dbModels.{BatchJobInstanceDBModel, BatchJobInstanceDBModelV1}
import it.agilelab.bigdata.wasp.repository.core.mappers.{BatchJobInstanceMapperV1}
import org.bson.{BsonReader, BsonWriter}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.codecs.configuration.{CodecProvider, CodecRegistry}
import org.mongodb.scala.bson.{BsonDocument, BsonInt64, BsonString}


object BatchJobInstanceDBProvider extends CodecProvider {

  override def get[T](clazz: Class[T], registry: CodecRegistry): Codec[T] = {
    val codecBsonDocument: Codec[BsonDocument] = registry.get(classOf[BsonDocument])
    if (clazzOf.isAssignableFrom(clazz)) {
      new Codec[T] {
        override def decode(reader: BsonReader, decoderContext: DecoderContext): T = {
          val bsonDocument = codecBsonDocument.decode(reader, decoderContext)
          val version = bsonDocument.getString("version").getValue
          version match {
            case BatchJobInstanceMapperV1.version =>
              BatchJobInstanceDBModelV1(
                name = bsonDocument.get("name").asString().getValue,
                instanceOf = bsonDocument.get("instanceOf").asString().getValue,
                startTimestamp = bsonDocument.get("startTimestamp").asInt64().getValue,
                currentStatusTimestamp = bsonDocument.get("currentStatusTimestamp").asInt64().getValue,
                status = JobStatus.withName(bsonDocument.get("status").asString().getValue),
                restConfig = ConfigFactory.parseString(bsonDocument.get("restConfig").asString.getValue),
                error = if (bsonDocument.containsKey("error")) Some(bsonDocument.get("error").asString().getValue) else None
              ).asInstanceOf[T]
          }
        }
        override def encode(writer: BsonWriter, value: T, encoderContext: EncoderContext): Unit = {
          var version: String = null // reference holder
          val instance = value match {
            case x: BatchJobInstanceDBModelV1 =>
              version = BatchJobInstanceMapperV1.version
              x.asInstanceOf[BatchJobInstanceDBModelV1]
            case _ => throw new Exception("Model for this value does not exist")
          }
          val document = BsonDocument()
            .append("name", BsonString(instance.name))
            .append("instanceOf", BsonString(instance.instanceOf))
            .append("startTimestamp", BsonInt64(instance.startTimestamp))
            .append("currentStatusTimestamp", BsonInt64(instance.currentStatusTimestamp))
            .append("status", BsonString(instance.status.toString))
            .append("restConfig", BsonString(instance.restConfig.root.render(ConfigRenderOptions.concise())))
            .append("version", BsonString(version))

          val withError = instance.error.map(error => document.append("error", BsonString(error)))
            .getOrElse(document)

          codecBsonDocument.encode(writer, withError, encoderContext)
        }

        override def getEncoderClass: Class[T] = clazz
      }
    } else {
      null
    }
  }

  def clazzOf: Class[BatchJobInstanceDBModel] = classOf[BatchJobInstanceDBModel]
}
