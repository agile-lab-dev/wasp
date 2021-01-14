package it.agilelab.bigdata.wasp.core.utils

import java.nio.charset.StandardCharsets
import java.util.Base64

import com.sksamuel.avro4s.{RecordFormat, SchemaFor}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.scalatest.FlatSpec

case class JsonAvroConverterTestData(id: String, payload: Array[Byte])

class JsonAvroConverterTest extends FlatSpec {

  it should "be able to deserialize base64 encoded byte arrays" in {
    val payloadArray = Range.apply(0, 10).map(_.toByte).toArray
    val schema = SchemaFor[JsonAvroConverterTestData].apply()
    val payload = new String(Base64.getEncoder.encode(payloadArray), StandardCharsets.UTF_8)
    val jsonRepr =
      s"""{
         | "id": "myid",
         | "payload": "$payload"
         |}""".stripMargin


    val converter = new JsonAvroConverter()

    val result = converter.convertToAvro(jsonRepr.getBytes(StandardCharsets.UTF_8), schema, None)

    val c = new GenericDatumReader[GenericRecord](schema)

    val result1= c.read(null, DecoderFactory.get().createBinaryDecoder(result, 0, result.size, null))

    val deserialized = RecordFormat[JsonAvroConverterTestData].from(result1)

    assert(deserialized.payload.toList == payloadArray.toList)
  }

}
