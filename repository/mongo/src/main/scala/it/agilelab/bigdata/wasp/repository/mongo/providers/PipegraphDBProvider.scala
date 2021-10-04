package it.agilelab.bigdata.wasp.repository.mongo.providers


import it.agilelab.bigdata.wasp.models.{PipegraphStatus, StrategyModel, StreamingReaderModel, StructuredStreamingETLModel, WriterModel}
import it.agilelab.bigdata.wasp.repository.core.dbModels.{PipegraphDBModel, PipegraphDBModelV1, PipegraphInstanceDBModel, PipegraphInstanceDBModelV1}
import it.agilelab.bigdata.wasp.repository.core.mappers.{ PipegraphInstanceMapperV1}
import org.bson.{BsonDocumentWriter, BsonReader, BsonWriter}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.codecs.configuration.{CodecProvider, CodecRegistry}
import org.mongodb.scala.bson.{BsonDocument, BsonInt64, BsonString}


object PipegraphInstanceDBModelProvider extends CodecProvider{

  override def get[T](clazz: Class[T], registry: CodecRegistry): Codec[T] = {
    val codecBsonDocument: Codec[BsonDocument] = registry.get(classOf[BsonDocument])
    if (clazzOf.isAssignableFrom(clazz)) {
      new Codec[T] {
        override def decode(reader: BsonReader, decoderContext: DecoderContext): T = {
          val bsonDocument = codecBsonDocument.decode(reader, decoderContext)
          val version = bsonDocument.getString("version").getValue
          version match {
            case PipegraphInstanceMapperV1.version =>
              PipegraphInstanceDBModelV1(
                name = bsonDocument.get("name").asString().getValue,
                instanceOf = bsonDocument.get("instanceOf").asString().getValue,
                startTimestamp = bsonDocument.get("startTimestamp").asInt64().getValue,
                currentStatusTimestamp = bsonDocument.get("currentStatusTimestamp").asInt64().getValue,
                status = PipegraphStatus.withName(bsonDocument.get("status").asString().getValue),
                executedByNode =
                  if (bsonDocument.containsKey("executedByNode")) Some(bsonDocument.get("executedByNode").asString().getValue)
                  else None,
                peerActor =
                  if (bsonDocument.containsKey("peerActor")) Some(bsonDocument.get("peerActor").asString().getValue)
                  else None,
                error = if (bsonDocument.containsKey("error")) Some(bsonDocument.get("error").asString().getValue) else None
              ).asInstanceOf[T]
            case _ => throw new Exception("This version of a mapper does not exist")
          }
        }
        override def encode(writer: BsonWriter, value: T, encoderContext: EncoderContext): Unit = {
            var version : String = null // reference holder
            val instance = value match {
              case x: PipegraphInstanceDBModelV1 =>
                version = PipegraphInstanceMapperV1.version
                value.asInstanceOf[PipegraphInstanceDBModelV1]
              case _ => throw new Exception("Another Model version does not exist")
            }
            val document = BsonDocument()
              .append("name", BsonString(instance.name))
              .append("instanceOf", BsonString(instance.instanceOf))
              .append("startTimestamp", BsonInt64(instance.startTimestamp))
              .append("currentStatusTimestamp", BsonInt64(instance.currentStatusTimestamp))
              .append("status", BsonString(instance.status.toString))
              .append("version", BsonString(version))

            val withError = instance.error
              .map(error => document.append("error", BsonString(error)))
              .getOrElse(document)

            val withExecuted = instance.executedByNode
              .map(node => document.append("executedByNode", BsonString(node)))
              .getOrElse(withError)

            val withPeer = instance.peerActor
              .map(peer => document.append("peerActor", BsonString(peer)))
              .getOrElse(withExecuted)

          codecBsonDocument.encode(writer, withPeer, encoderContext)
        }

        override def getEncoderClass: Class[T] = clazz
      }
    } else {
      null
    }
  }

  def clazzOf: Class[PipegraphInstanceDBModel] = classOf[PipegraphInstanceDBModel]

  def createBsonDocument[T](codec: Codec[T], version: String, value: T, encoderContext: EncoderContext): BsonDocument = {
    val bsonDocWriter = new BsonDocumentWriter(new BsonDocument("version", BsonString(version)))
    codec.encode(bsonDocWriter, value, encoderContext)
    bsonDocWriter.getDocument
  }
}

//case object PipegraphInstanceDBModelV1Provider{
//  val additionalCodecs = fromProviders(
//    createCodecProvider[PipegraphStatus]()
//  )
//  val codecRegistry = fromRegistries(
//    additionalCodecs,
//    DEFAULT_CODEC_REGISTRY
//  )
//  val pipegraphInstanceDBModelV1Codec = createCodec[PipegraphInstanceDBModelV1]()
//}