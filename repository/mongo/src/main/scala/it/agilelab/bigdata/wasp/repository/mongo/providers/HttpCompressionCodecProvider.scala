package it.agilelab.bigdata.wasp.repository.mongo.providers

import it.agilelab.bigdata.wasp.models.HttpCompression
import org.bson.codecs.configuration.{CodecProvider, CodecRegistry}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.{BsonReader, BsonWriter}

object HttpCompressionCodecProvider extends CodecProvider with Codec[HttpCompression] {


  override def get[T](clazz: Class[T], registry: CodecRegistry): Codec[T] = {
    if (classOf[HttpCompression].isAssignableFrom(clazz)) {
      this.asInstanceOf[Codec[T]]
    } else {
      null
    }
  }

  override def getEncoderClass: Class[HttpCompression] = classOf[HttpCompression]

  override def encode(writer: BsonWriter, value: HttpCompression, encoderContext: EncoderContext): Unit = {
    val error = (x: HttpCompression) => throw new IllegalArgumentException(s"Http compression $x should be added to the object to string map")
    val s = HttpCompression.asString
                            .applyOrElse(value, error)
    writer.writeString(s)
  }

  override def decode(reader: BsonReader, decoderContext: DecoderContext): HttpCompression = {
    val error = (x: String) =>throw new IllegalArgumentException(s"Http compression as string [$x] has no corresponding ")
    val value = reader.readString()
    HttpCompression.fromString.applyOrElse(value, error)
  }
}
