package it.agilelab.bigdata.wasp.core.utils

import it.agilelab.bigdata.wasp.core.utils.SealedTraitCodecProvider.TYPE_FIELD
import org.bson.codecs.configuration.{CodecProvider, CodecRegistry}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.{BsonReader, BsonType, BsonWriter}

import scala.collection.mutable

abstract class AbstractCodecProvider[C] extends CodecProvider {
  override def get[T](clazz: Class[T], registry: CodecRegistry): Codec[T] = {
    if (clazz == clazzOf) {
      new Codec[T] {
        override def decode(reader: BsonReader, decoderContext: DecoderContext): T = {
          implicit val r: BsonReader = reader
          implicit val ctx: DecoderContext = decoderContext
          reader.readStartDocument()
          val result = decodeClass(registry)
          reader.readEndDocument()
          result.asInstanceOf[T]
        }

        override def encode(writer: BsonWriter, value: T, encoderContext: EncoderContext): Unit = {
          implicit val w: BsonWriter = writer
          implicit val ctx: EncoderContext = encoderContext
          val instanceValue = value.asInstanceOf[C]
          writer.writeStartDocument()
          encodeClass(registry, instanceValue)
          writer.writeEndDocument()
        }

        override def getEncoderClass: Class[T] = clazz
      }
    } else {
      null
    }
  }

  def decodeClass(registry: CodecRegistry)
                 (implicit reader: BsonReader, decoderContext: DecoderContext): C

  def encodeClass(registry: CodecRegistry, value: C)
                 (implicit writer: BsonWriter, encoderContext: EncoderContext): Unit

  def clazzOf: Class[C]

  protected def readObject[T](name: String, codec: Codec[T])
                             (implicit reader: BsonReader, decoderContext: DecoderContext): T = {
    reader.readName(name)
    codec.decode(reader, decoderContext)
  }

  protected def readList[T](name: String, codec: Codec[T])
                           (implicit reader: BsonReader, decoderContext: DecoderContext): List[T] = {
    reader.readName(name)
    val buffer = mutable.ListBuffer.empty[T]
    reader.readStartArray()
    while (reader.readBsonType() == BsonType.DOCUMENT) {
      buffer += codec.decode(reader, decoderContext)
    }
    reader.readEndArray()

    buffer.toList
  }

  protected def writeList[T](name: String, values: List[T], codec: Codec[T])
                            (implicit writer: BsonWriter, encoderContext: EncoderContext): Unit = {
    writer.writeStartArray(name)
    values.foreach { x =>
      codec.encode(writer, x, encoderContext)
    }
    writer.writeEndArray()
  }

  protected def writeObject[T](name: String, value: T, codec: Codec[T])
                              (implicit writer: BsonWriter, encoderContext: EncoderContext): Unit = {
    writer.writeName(name)
    codec.encode(writer, value, encoderContext)
  }

}
