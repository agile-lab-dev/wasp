package it.agilelab.bigdata.wasp.repository.mongo.providers

import org.bson.{BsonDocumentWriter, BsonReader, BsonWriter}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.codecs.configuration.{CodecProvider, CodecRegistry}
import org.mongodb.scala.bson.{BsonDocument, BsonString}

import scala.reflect.ClassTag

object VersionedCodecProvider {

  def makeApplyMethod(num: Int): String ={
    var s = "def apply[SuperClass: ClassTag, "
    for(i <- 1 to num){
      s = s + s"SubClass${i} <: SuperClass, "
    }
    s = s.dropRight(2) + "]"

    s = s + "(versionExtractor: SuperClass => String, a: Class[_ <: SuperClass],"
    for(i <- 1 to num){
      s = s + s"x${i}: (String, Codec[Subclass${i}]), "
    }

    s = s.dropRight(2) + ")\n"
    s = s + ": VersionedCodecProvider[SuperClass] = {\n" + "val map: Map[String, Codec[SuperClass]] = Map("

    for(i <- 1 to num){
      s = s + s"x${i}._1 -> x${i}._2.asInstanceOf[Codec[SuperClass]], "
    }
    s = s.dropRight(2) + ")\n" + "new VersionedCodecProvider[SuperClass](map, versionExtractor) \n}"
    s
  }

  def apply[SuperClass: ClassTag, SubClass <: SuperClass](versionExtractor: SuperClass => String, a: Class[_ <: SuperClass], s: (String, Codec[SubClass])): VersionedCodecProvider[SuperClass] ={
    val map: Map[String, Codec[SuperClass]] = Map(s._1 -> s._2.asInstanceOf[Codec[SuperClass]])
    new VersionedCodecProvider[SuperClass](map, versionExtractor)
  }

  def apply[SuperClass: ClassTag, SubClass <: SuperClass, SubClass1 <: SuperClass](versionExtractor: SuperClass => String, a: Class[_ <: SuperClass], s: (String, Codec[SubClass]), x: (String, Codec[SubClass1])): VersionedCodecProvider[SuperClass] ={
    val map: Map[String, Codec[SuperClass]] = Map(s._1 -> s._2.asInstanceOf[Codec[SuperClass]], x._1 -> x._2.asInstanceOf[Codec[SuperClass]])
    new VersionedCodecProvider[SuperClass](map, versionExtractor)
  }
}

class VersionedCodecProvider[T](map: Map[String, Codec[T]], versionExtractor: T => String)(implicit classTag: ClassTag[T]) extends CodecProvider {

  def clazzOf: Class[T] = classTag.runtimeClass.asInstanceOf[Class[T]]

  override def get[R](clazz: Class[R], registry: CodecRegistry): Codec[R] = {
    val codecBsonDocument: Codec[BsonDocument] = registry.get(classOf[BsonDocument])

    if (clazzOf.isAssignableFrom(clazz)) {
      new Codec[R] {
        override def decode(reader: BsonReader, decoderContext: DecoderContext): R = {
          val bsonDoc = codecBsonDocument.decode(reader, decoderContext)
          val version = bsonDoc.getString("version").getValue
          val codec = map.getOrElse(version, throw new Exception("no decoder available"))
          codec.decode(bsonDoc.asBsonReader(), decoderContext).asInstanceOf[R]
        }

        override def encode(writer: BsonWriter, value: R, encoderContext: EncoderContext): Unit = {
          val version = versionExtractor(value.asInstanceOf[T])
          val codec = map.getOrElse(version, throw new Exception("no encoder available"))
          val bsonDocument = createBsonDocument(codec.asInstanceOf[Codec[T]], version, value.asInstanceOf[T], encoderContext)
          codecBsonDocument.encode(writer, bsonDocument, encoderContext)
        }
        override def getEncoderClass: Class[R] = clazz
      }
    }
    else
    {
      null
    }
  }

  def createBsonDocument(codec: Codec[T], version: String, value: T, encoderContext: EncoderContext): BsonDocument = {
    val bsonDocWriter = new BsonDocumentWriter(new BsonDocument("version", BsonString(version)))
    codec.encode(bsonDocWriter, value, encoderContext)
    bsonDocWriter.getDocument
  }
}