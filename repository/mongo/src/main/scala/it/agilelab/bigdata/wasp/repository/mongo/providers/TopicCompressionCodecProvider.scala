package it.agilelab.bigdata.wasp.repository.mongo.providers

import org.bson.codecs.configuration.{CodecProvider, CodecRegistry}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.{BsonReader, BsonWriter}
import it.agilelab.bigdata.wasp.models.TopicCompression

object TopicCompressionCodecProvider extends CodecProvider with Codec[TopicCompression] {


  override def get[T](clazz: Class[T], registry: CodecRegistry): Codec[T] = {
    if (classOf[TopicCompression].isAssignableFrom(clazz)) {
      this.asInstanceOf[Codec[T]]
    } else {
      null
    }
  }

  override def getEncoderClass: Class[TopicCompression] = classOf[TopicCompression]

  override def encode(writer: BsonWriter, value: TopicCompression, encoderContext: EncoderContext): Unit = {
    val error = (x: TopicCompression) => throw new IllegalArgumentException(s"Topic compression $x should be added to the object to string map")
    val s = TopicCompression.asString
                            .applyOrElse(value, error)
    writer.writeString(s)
  }

  override def decode(reader: BsonReader, decoderContext: DecoderContext): TopicCompression = {
    val error = (x: String) =>throw new IllegalArgumentException(s"Topic compression as string [$x] has no corresponding ")
    val value = reader.readString()
    TopicCompression.fromString.applyOrElse(value, error)
  }
}



