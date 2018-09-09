package it.agilelab.bigdata.wasp.consumers.spark.plugins.console

import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.readers.SparkReader
import it.agilelab.bigdata.wasp.consumers.spark.writers._
import it.agilelab.bigdata.wasp.core.datastores.DatastoreProduct
import it.agilelab.bigdata.wasp.core.datastores.DatastoreProduct.ConsoleProduct
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.configuration.ValidationRule
import it.agilelab.bigdata.wasp.core.models.{LegacyStreamingETLModel, ReaderModel, StructuredStreamingETLModel, WriterModel}
import it.agilelab.bigdata.wasp.core.utils.WaspDB
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

class ConsoleConsumersSpark extends WaspConsumersSparkPlugin with Logging {

  override def datastoreProduct: DatastoreProduct = ConsoleProduct

  override def initialize(waspDB: WaspDB): Unit = {
    logger.info(s"Initialized plugin for $datastoreProduct")
  }

  override def getValidationRules: Seq[ValidationRule] = Seq()

  override def getSparkLegacyStreamingWriter(ssc: StreamingContext,
                                             legacyStreamingETLModel: LegacyStreamingETLModel,
                                             writerModel: WriterModel): SparkLegacyStreamingWriter = {
    logger.info(s"Creating $datastoreProduct Spark Legacy Streaming writer")
    new ConsoleSparkLegacyStreamingWriter()
  }

  override def getSparkStructuredStreamingWriter(ss: SparkSession,
                                                 structuredStreamingETLModel: StructuredStreamingETLModel,
                                                 writerModel: WriterModel): ConsoleSparkStructuredStreamingWriter = {
    logger.info(s"Creating $datastoreProduct Spark Structured Streaming writer")
    new ConsoleSparkStructuredStreamingWriter()
  }

  override def getSparkBatchWriter(sc: SparkContext, writerModel: WriterModel): SparkWriter = {
    logger.info(s"Creating $datastoreProduct Spark batch writer")
    new ConsoleSparkWriter()
  }

  override def getSparkBatchReader(sc: SparkContext, readerModel: ReaderModel): SparkReader = {
    val msg = s"The datastore product $datastoreProduct is not a valid batch source! Reader model $readerModel is not valid."
    logger.error(msg)
    throw new UnsupportedOperationException(msg)
  }
}