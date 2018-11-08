package it.agilelab.bigdata.wasp.core.utils

import com.typesafe.config.ConfigFactory
import it.agilelab.darwin.manager.AvroSchemaManager
import it.agilelab.bigdata.wasp.core.logging.Logging
import collection.JavaConversions._


object AvroToJsonUtil extends Logging {

  val configAvroSchemaManager = ConfigManager.getAvroSchemaManagerConfig

  AvroSchemaManager.instance(configAvroSchemaManager)
  val jsonAvroConverter = new JsonAvroConverter()

  def jsonToAvro(json: String, schemaStr: String, useAvroSchemaManager: Boolean): Array[Byte] = {
    logger.debug("Starting jsonToAvro encoding ...")
    jsonAvroConverter.convertToAvro(json.getBytes, schemaStr, useAvroSchemaManager)
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