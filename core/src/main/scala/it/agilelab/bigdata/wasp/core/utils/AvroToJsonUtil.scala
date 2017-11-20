package it.agilelab.bigdata.wasp.core.utils

import java.io.{ByteArrayInputStream, _}

import it.agilelab.bigdata.wasp.core.logging.Logging
import org.apache.avro.Schema
import org.apache.avro.file.{DataFileStream, DataFileWriter}
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.DecoderFactory

//TODO re-enable refactorization but it has bug on nested field with UNION null, complexType.
//TODO https://github.com/allegro/json-avro-converter/issues/9
//object AvroToJsonUtil extends Logging {
//  val jsonAvroConverter = new JsonAvroConverter()
//
//  def jsonToAvro(json: String, schemaStr: String): Array[Byte] = {
//    logger.debug("Starting jsonToAvro encoding ...")
//    jsonAvroConverter.convertToAvro(json.getBytes, schemaStr)
//  }
//
//  def avroToJson(avro: Array[Byte], schemaStr: String): String = {
//    logger.debug("Starting avroToJson encoding ...")
//    new String(jsonAvroConverter.convertToJson(avro, schemaStr), "UTF-8")
//  }
//
//  //Use this function all the times you need to pass a Json generic text message to the Avro encoder. This way, afterward, the decoder won't get broken.
//  def convertToUTF8(s: String): String = {
//    //s.replaceAll("""""","""\"""")
//    s.replaceAll("#", "").replaceAll("\\\\", "").replaceAll("\"", "").replaceAll( """/[^a-z 0-9\.\:\;\!\?]+/gi""", " ").replaceAll( """[^\p{L}\p{Nd}\.\:\;\!\?]+""", " ")
//  }
//
//}

object AvroToJsonUtil extends Logging {

  def jsonToAvro(json: String, schemaStr: String): Array[Byte] = {
    logger.debug("Starting jsonToAvro encoding ...")

    var encoder = null

    val schema = new Schema.Parser().parse(schemaStr)
    val reader = new GenericDatumReader[GenericRecord](schema)
    val input = new ByteArrayInputStream(json.getBytes("UTF-8"))
    val output = new ByteArrayOutputStream()
    val din = new DataInputStream(input)

    var writer = new DataFileWriter(new GenericDatumWriter[GenericRecord]())
    writer.create(schema, output)
    val decoder = DecoderFactory.get().jsonDecoder(schema, din)
    var datum: GenericRecord = null
    var t = true

    while (t) {

      try {
        datum = reader.read(null, decoder)
      }
      catch {
        case e: EOFException => t = false
      }

      writer.append(datum)
    }

    writer.flush()
    output.toByteArray
  }

  def avroToJson(avro: Array[Byte]): String = {
    logger.debug("Starting avroToJson encoding ...")

    val pretty = false
    val JsonEncoder = null

    val reader = new GenericDatumReader[GenericRecord]()
    val input = new ByteArrayInputStream(avro)
    val streamReader = new DataFileStream[GenericRecord](input, reader)
    val output = new ByteArrayOutputStream()

    val schema: Schema = streamReader.getSchema
    val writer = new GenericDatumWriter[GenericRecord](schema)
    val encoder = new SimpleUnionJsonEncoder(schema, output)

    while (streamReader.iterator.hasNext) {
      writer.write(streamReader.iterator().next(), encoder)
    }

    encoder.flush()
    output.flush()
    new String(output.toByteArray, "UTF-8")
  }

  //Use this function all the times you need to pass a Json generic text message to the Avro encoder. This way, afterward, the decoder won't get broken.
  def convertToUTF8(s: String): String = {
    //s.replaceAll("""""","""\"""")
    s.replaceAll("#", "").replaceAll("\\\\", "").replaceAll("\"", "").replaceAll( """/[^a-z 0-9\.\:\;\!\?]+/gi""", " ").replaceAll( """[^\p{L}\p{Nd}\.\:\;\!\?]+""", " ")
  }

}