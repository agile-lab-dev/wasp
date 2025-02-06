package it.agilelab.bigdata.wasp.consumers.spark.http.data

import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{BinaryDecoder, BinaryEncoder, DatumReader, DatumWriter, DecoderFactory, EncoderFactory}
import org.apache.avro.{Schema, SchemaBuilder}

import java.io.ByteArrayOutputStream

case class FromKafka(id: String, exampleAuthor: String, timestamp: Long)

object FromKafka {
  val schema: Schema = SchemaBuilder
    .record("FromKafka")
    .fields()
    .name("id")
    .`type`()
    .stringType()
    .noDefault()
    .name("exampleAuthor")
    .`type`()
    .stringType()
    .noDefault()
    .name("timestamp")
    .`type`()
    .longType()
    .noDefault()
    .endRecord();

  def toRecord(obj: FromKafka): GenericRecord = {
    val record = new GenericData.Record(schema)
    record.put("id", obj.id)
    record.put("exampleAuthor", obj.exampleAuthor)
    record.put("timestamp", obj.timestamp)
    record
  }

  def fromRecord(record: GenericRecord): FromKafka = {
    FromKafka(
      record.get("id").toString,
      record.get("exampleAuthor").toString,
      record.get("timestamp").asInstanceOf[Long]
    )
  }

  def serializeToBytes(obj: FromKafka, schema: Schema): Array[Byte] = {
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
  ): FromKafka = {
    val byteArrayInputStream = new java.io.ByteArrayInputStream(bytes)
    val datumReader: DatumReader[GenericRecord] =
      new GenericDatumReader[GenericRecord](schema)
    val decoder: BinaryDecoder =
      DecoderFactory.get().binaryDecoder(byteArrayInputStream, null)
    val record = datumReader.read(null, decoder)
    fromRecord(record)
  }
}
