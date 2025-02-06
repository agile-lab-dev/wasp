package it.agilelab.bigdata.wasp.consumers.spark.http.data

import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{BinaryDecoder, BinaryEncoder, DatumReader, DatumWriter, DecoderFactory, EncoderFactory}

import java.io.ByteArrayOutputStream

case class SampleData(id: String, text: String)

object SampleData {
  val schema: Schema = SchemaBuilder
    .record("SampleData")
    .fields()
    .name("id")
    .`type`()
    .stringType()
    .noDefault()
    .name("text")
    .`type`()
    .stringType()
    .noDefault()
    .endRecord();

  def toRecord(obj: SampleData): GenericRecord = {
    val record = new GenericData.Record(schema)
    record.put("id", obj.id)
    record.put("text", obj.text)
    record
  }

  def fromRecord(record: GenericRecord): SampleData = {
    SampleData(
      record.get("id").toString,
      record.get("text").toString,
    )
  }

  def serializeToBytes(obj: SampleData, schema: Schema): Array[Byte] = {
    val byteArrayOutputStream = new ByteArrayOutputStream()
    val datumWriter: DatumWriter[GenericRecord] =
      new GenericDatumWriter[GenericRecord](schema)
    val encoder: BinaryEncoder =
      EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null)
    datumWriter.write(toRecord(obj), encoder)
    encoder.flush()
    byteArrayOutputStream.toByteArray // Return the byte array
  }

  def deserializeFromBytes(
                            bytes: Array[Byte],
                            schema: Schema
                          ): SampleData = {
    val byteArrayInputStream = new java.io.ByteArrayInputStream(bytes)
    val datumReader: DatumReader[GenericRecord] =
      new GenericDatumReader[GenericRecord](schema)
    val decoder: BinaryDecoder =
      DecoderFactory.get().binaryDecoder(byteArrayInputStream, null)
    val record = datumReader.read(null, decoder)
    fromRecord(record)
  }
}