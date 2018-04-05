package it.agilelab.bigdata.wasp.consumers.spark.plugins.console

import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.readers.SparkReader
import it.agilelab.bigdata.wasp.consumers.spark.writers._
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.configuration.ValidationRule
import it.agilelab.bigdata.wasp.core.models.{Datastores, WriterModel}
import it.agilelab.bigdata.wasp.core.utils.WaspDB
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

class ConsoleConsumersSpark extends WaspConsumersSparkPlugin with Logging {

  override def initialize(waspDB: WaspDB): Unit = {
    logger.info(s"Initialize the console BL (nothing)")
  }

  override def getSparkLegacyStreamingWriter(ssc: StreamingContext, writerModel: WriterModel): SparkLegacyStreamingWriter = {
    logger.info(s"Initialize the console spark streaming writer")
    new ConsoleSparkLegacyStreamingWriter()
  }

  override def getSparkStructuredStreamingWriter(ss: SparkSession, writerModel: WriterModel) = {
    logger.info(s"Initialize the console spark structured streaming writer")
    new ConsoleSparkStructuredStreamingWriter()
  }

  override def getSparkWriter(sc: SparkContext, writerModel: WriterModel): SparkWriter = {
    logger.info(s"Initialize the console spark batch writer")
    new ConsoleSparkWriter()
  }

  override def getSparkReader(id: String, name: String): SparkReader = {
    val msg = s"Invalid spark reader type: console - name: $name"
    logger.error(msg)
    throw new UnsupportedOperationException(msg)
  }

  override def pluginType: String = Datastores.consoleProduct

  override def getValidationRules: Seq[ValidationRule] = Seq()
}