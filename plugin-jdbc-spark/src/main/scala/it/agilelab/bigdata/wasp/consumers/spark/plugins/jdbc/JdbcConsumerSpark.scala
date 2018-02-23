package it.agilelab.bigdata.wasp.consumers.spark.plugins.jdbc

import java.net.URI

import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.readers.SparkReader
import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkLegacyStreamingWriter, SparkStructuredStreamingWriter, SparkWriter}
import it.agilelab.bigdata.wasp.core.bl.{SqlSourceBl, SqlSourceBlImpl}
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.{Datastores, SqlSourceModel, WriterModel}
import it.agilelab.bigdata.wasp.core.utils.WaspDB
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

class JdbcConsumerSpark extends WaspConsumersSparkPlugin with Logging {

  var sqlModelBL: SqlSourceBl = _

  override def initialize(waspDB: WaspDB): Unit = {
    logger.info("Initializing Jdbc consumer spark")
    sqlModelBL = new SqlSourceBlImpl(waspDB)
  }

  //TODO implement writer
  override def getSparkLegacyStreamingWriter(ssc: StreamingContext, writerModel: WriterModel): SparkLegacyStreamingWriter = ???

  //TODO implement writer
  override def getSparkStructuredStreamingWriter(ss: SparkSession, writerModel: WriterModel): SparkStructuredStreamingWriter = ???

  //TODO implement writer
  override def getSparkWriter(sc: SparkContext, writerModel: WriterModel): SparkWriter = ???

  override def getSparkReader(id: String, name: String): SparkReader = {
    // get the sql model using the provided id & bl
    val sqlModelOpt = sqlModelBL.getByName(name)
    // if we found a model, return the correct reader
    val sqlModel: SqlSourceModel = if (sqlModelOpt.isDefined) {
      sqlModelOpt.get
    } else {
      throw new Exception(s"Raw model not found: $name")
    }
    new JdbcSparkReader(sqlModel)
  }

  override def pluginType: String = Datastores.jdbcProduct
}
