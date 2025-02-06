package it.agilelab.bigdata.wasp.core.utils

import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.avro.util.Utf8
import org.scalatest.FlatSpec

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.Base64

case class JsonAvroConverterTestData(id: String, payload: Array[Byte])
object JsonAvroConverterTestData {
  val schema = SchemaBuilder
    .builder()
    .record("JsonAvroConverterTestData")
    .fields()
    .requiredString("id")
    .requiredBytes("payload")
    .endRecord()
}
class JsonAvroConverterTest extends FlatSpec {

  it should "be able to deserialize base64 encoded byte arrays" in {
    val payloadArray = Range.apply(0, 10).map(_.toByte).toArray
    val schema       = JsonAvroConverterTestData.schema
    val payload      = new String(Base64.getEncoder.encode(payloadArray), StandardCharsets.UTF_8)
    val jsonRepr =
      s"""{
         | "id": "myid",
         | "payload": "$payload"
         |}""".stripMargin

    val converter = new JsonAvroConverter()

    val result = converter.convertToAvro(jsonRepr.getBytes(StandardCharsets.UTF_8), schema, None)

    val c = new GenericDatumReader[GenericRecord](schema)

    val result1 = c.read(null, DecoderFactory.get().binaryDecoder(result, 0, result.size, null))

    val deserialized = {
      JsonAvroConverterTestData(
        result1.get("id").asInstanceOf[Utf8].toString,
        result1.get("payload").asInstanceOf[ByteBuffer].array()
      )
    }

    assert(deserialized.payload.toList == payloadArray.toList)
  }

}
