package it.agilelab.bigdata.wasp.consumers.spark.plugins.jdbc

import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.readers.SparkReader
import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkLegacyStreamingWriter, SparkStructuredStreamingWriter, SparkWriter}
import it.agilelab.bigdata.wasp.core.bl.{SqlSourceBl, SqlSourceBlImpl}
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.{Datastores, WriterModel}
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

  override def getSparkLegacyStreamingWriter(ssc: StreamingContext, writerModel: WriterModel): SparkLegacyStreamingWriter = {
    val msg = s"Invalid spark writer type: jdbc spark streaming writer"
    logger.error(msg)
    throw new UnsupportedOperationException(msg)
  }

  override def getSparkStructuredStreamingWriter(ss: SparkSession, writerModel: WriterModel): SparkStructuredStreamingWriter = {
    val msg = s"Invalid spark writer type: jdbc spark structured streaming writer"
    logger.error(msg)
    throw new UnsupportedOperationException(msg)
  }

  override def getSparkWriter(sc: SparkContext, writerModel: WriterModel): SparkWriter = {
    val msg = s"Invalid spark writer type: jdbc spark batch writer"
    logger.error(msg)
    throw new UnsupportedOperationException(msg)
  }

  override def getSparkReader(endpointId: String, name: String): SparkReader = {
    logger.info(s"Initialize JdbcReader with this id: '$endpointId' and name: '$name'")
    val sqlOpt = sqlModelBL.getByName(endpointId)
    val sqlSource =
      if (sqlOpt.isDefined) {
        sqlOpt.get
      } else {
        val msg = s"Jdbc spark reader sqlOption not found - id: '$endpointId, name: $name'"
        logger.error(msg)
        throw new Exception(msg)
      }

    new JdbcSparkReader(sqlSource)
  }

  override def pluginType: String = Datastores.jdbcProduct
}