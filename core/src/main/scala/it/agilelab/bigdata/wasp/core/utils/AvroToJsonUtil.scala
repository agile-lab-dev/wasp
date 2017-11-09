package it.agilelab.bigdata.wasp.core.utils

import java.io.{ByteArrayInputStream, _}

import it.agilelab.bigdata.wasp.core.logging.Logging
import org.apache.avro.Schema
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}

object AvroToJsonUtil extends Logging {
  val jsonAvroConverter = new JsonAvroConverter()

  def jsonToAvro(json: String, schemaStr: String): Array[Byte] = {
    logger.debug("Starting jsonToAvro encoding ...")
    jsonAvroConverter.convertToAvro(json.getBytes, schemaStr)
  }

  def avroToJson(avro: Array[Byte], schemaStr: String): String = {
    logger.debug("Starting avroToJson encoding ...")
    new String(jsonAvroConverter.convertToJson(avro, schemaStr), "UTF-8")
  }

  //Use this function all the times you need to pass a Json generic text message to the Avro encoder. This way, afterward, the decoder won't get broken.
  def convertToUTF8(s: String): String = {
    //s.replaceAll("""""","""\"""")
    s.replaceAll("#", "").replaceAll("\\\\", "").replaceAll("\"", "").replaceAll( """/[^a-z 0-9\.\:\;\!\?]+/gi""", " ").replaceAll( """[^\p{L}\p{Nd}\.\:\;\!\?]+""", " ")
  }

}