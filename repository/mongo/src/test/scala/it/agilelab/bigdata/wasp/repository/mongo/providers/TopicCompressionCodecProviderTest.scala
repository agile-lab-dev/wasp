package it.agilelab.bigdata.wasp.repository.mongo.providers

import java.io.StringWriter

import it.agilelab.bigdata.wasp.models.TopicCompression
import org.bson.codecs.Codec
import org.bson.json.{JsonReader, JsonWriter}
import org.bson.{BsonReader, BsonWriter}
import org.scalatest.FunSuite

class TopicCompressionCodecProviderTest extends FunSuite {


  test("Topic Compression codec provider should be able to handle case objects encoding") {

    val provider = TopicCompressionCodecProvider

    assert(provider.get(classOf[TopicCompression], null) !== null)
    assert(provider.get(classOf[String], null) === null)

    val codec = provider.get(classOf[TopicCompression], null)

    assert(write(codec, TopicCompression.Snappy) === """{"compression": "snappy"}""")

    assert(write(codec, TopicCompression.Gzip) === """{"compression": "gzip"}""")

    assert(write(codec, TopicCompression.Disabled) === """{"compression": "disabled"}""")
  }

  private def write(codec: Codec[TopicCompression], compression: TopicCompression) = {
    val stringW = new StringWriter()
    val writer: BsonWriter = new JsonWriter(stringW)
    writer.writeStartDocument()
    writer.writeName("compression")
    codec.encode(writer, compression, null)
    writer.writeEndDocument()
    writer.flush()
    stringW.toString
  }

  test("Topic Compression codec provider should be able to handle case objects decoding") {

    val provider = TopicCompressionCodecProvider

    assert(provider.get(classOf[TopicCompression], null) !== null)
    assert(provider.get(classOf[String], null) === null)

    val codec = provider.get(classOf[TopicCompression], null)

    assert(read(codec, """{"compression": "snappy"}""") === TopicCompression.Snappy)
    assert(read(codec, """{"compression": "gzip"}""") === TopicCompression.Gzip)
    assert(read(codec, """{"compression": "disabled"}""") === TopicCompression.Disabled)


    assertThrows[IllegalArgumentException](read(codec, """{ "compression" : "pippo" }"""))
  }

  private def read(codec: Codec[TopicCompression], compression: String): TopicCompression = {

    val reader: BsonReader = new JsonReader(compression)
    reader.readStartDocument()
    reader.readName("compression")
    val decoded = codec.decode(reader, null)
    reader.readEndDocument()
    decoded
  }
}
