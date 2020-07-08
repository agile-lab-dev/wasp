package it.agilelab.bigdata.wasp.db.mongo.providers

import org.bson.codecs.configuration.{CodecProvider, CodecRegistry}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.{BsonDocumentWriter, BsonReader, BsonString, BsonWriter}
import org.mongodb.scala.bson.BsonDocument
import it.agilelab.bigdata.wasp.db.mongo.providers.SealedTraitCodecProvider.TYPE_FIELD

abstract class SealedTraitCodecProvider[Trait] extends CodecProvider {

  override def get[T](clazz: Class[T], registry: CodecRegistry): Codec[T] = {
    val codecBsonDocument: Codec[BsonDocument] = registry.get(classOf[BsonDocument])
    if (clazz == clazzOf) {
      new Codec[T] {
        override def decode(reader: BsonReader, decoderContext: DecoderContext): T = {
          val bsonDoc = codecBsonDocument.decode(reader, decoderContext)
          decodeWithType(bsonDoc.getString(TYPE_FIELD).getValue, bsonDoc.asBsonReader(), decoderContext, registry)
            .asInstanceOf[T]
        }

        override def encode(writer: BsonWriter, value: T, encoderContext: EncoderContext): Unit = {
          val bsonDocument = encodeWithType(writer, value.asInstanceOf[Trait], encoderContext, registry)
          codecBsonDocument.encode(writer, bsonDocument, encoderContext)
        }

        override def getEncoderClass: Class[T] = clazz
      }
    } else {
      null
    }
  }

  def decodeWithType(classType: String, bsonReader: BsonReader, decoderContext: DecoderContext, registry: CodecRegistry): Trait

  def encodeWithType(bsonWriter: BsonWriter, value: Trait, encoderContext: EncoderContext, registry: CodecRegistry): BsonDocument

  def clazzOf: Class[Trait]

  def createBsonDocument[T](codec: Codec[T], typeKey: String, value: T, encoderContext: EncoderContext): BsonDocument = {
    val bsonDocWriter = new BsonDocumentWriter(new BsonDocument(TYPE_FIELD, new BsonString(typeKey)))
    codec.encode(bsonDocWriter, value, encoderContext)
    bsonDocWriter.getDocument
  }

}

object SealedTraitCodecProvider {
  val TYPE_FIELD: String = "@type"
}
