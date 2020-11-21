package it.agilelab.bigdata.wasp.repository.mongo.providers

import it.agilelab.bigdata.wasp.models.{SubjectStrategy, TopicCompression}
import org.bson.codecs.configuration.{CodecProvider, CodecRegistry}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.{BsonReader, BsonWriter}

object SubjectStrategyCodecProvider extends CodecProvider with Codec[SubjectStrategy] {


  override def get[T](clazz: Class[T], registry: CodecRegistry): Codec[T] = {
    if (classOf[SubjectStrategy].isAssignableFrom(clazz)) {
      this.asInstanceOf[Codec[T]]
    } else {
      null
    }
  }

  override def getEncoderClass: Class[SubjectStrategy] = classOf[SubjectStrategy]

  override def encode(writer: BsonWriter, value: SubjectStrategy, encoderContext: EncoderContext): Unit = {
    val error = (x: SubjectStrategy) => throw new IllegalArgumentException(s"Subject strategy $x should be added to the object to string map")
    val s = SubjectStrategy.asString
      .applyOrElse(value, error)
    writer.writeString(s)
  }

  override def decode(reader: BsonReader, decoderContext: DecoderContext): SubjectStrategy = {
    val error = (x: String) => throw new IllegalArgumentException(s"Subject strategy as string [$x] has no corresponding ")
    val value = reader.readString()
    SubjectStrategy.fromString.applyOrElse(value, error)
  }
}
