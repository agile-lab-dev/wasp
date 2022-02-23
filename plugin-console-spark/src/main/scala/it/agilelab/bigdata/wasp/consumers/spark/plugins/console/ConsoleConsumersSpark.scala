package it.agilelab.bigdata.wasp.consumers.spark.plugins.console

import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.readers.{SparkBatchReader, SparkStructuredStreamingReader}
import it.agilelab.bigdata.wasp.consumers.spark.writers._
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct.ConsoleProduct
import it.agilelab.bigdata.wasp.repository.core.db.WaspDB
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.configuration.ValidationRule
import it.agilelab.bigdata.wasp.models.{ReaderModel, StreamingReaderModel, StructuredStreamingETLModel, WriterModel}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

class ConsoleConsumersSpark extends WaspConsumersSparkPlugin with Logging {

  override def datastoreProduct: DatastoreProduct = ConsoleProduct

  override def initialize(waspDB: WaspDB): Unit = {
    logger.info(s"Initialized plugin for $datastoreProduct")
  }

  override def getValidationRules: Seq[ValidationRule] = Seq()

  override def getSparkStructuredStreamingWriter(ss: SparkSession,
                                                 structuredStreamingETLModel: StructuredStreamingETLModel,
                                                 writerModel: WriterModel): ConsoleSparkStructuredStreamingWriter = {
    logger.info(s"Creating $datastoreProduct Spark Structured Streaming writer")
    new ConsoleSparkStructuredStreamingWriter()
  }
  
  override def getSparkStructuredStreamingReader(ss: SparkSession,
                                                 structuredStreamingETLModel: StructuredStreamingETLModel,
                                                 streamingReaderModel: StreamingReaderModel): SparkStructuredStreamingReader = {
    val msg = s"The datastore product $datastoreProduct is not a valid streaming source! Reader model $streamingReaderModel is not valid."
    logger.error(msg)
    throw new UnsupportedOperationException(msg)
  }

  override def getSparkBatchWriter(sc: SparkContext, writerModel: WriterModel): SparkBatchWriter = {
    logger.info(s"Creating $datastoreProduct Spark batch writer")
    new ConsoleSparkBatchWriter()
  }

  override def getSparkBatchReader(sc: SparkContext, readerModel: ReaderModel): SparkBatchReader = {
    val msg = s"The datastore product $datastoreProduct is not a valid batch source! Reader model $readerModel is not valid."
    logger.error(msg)
    throw new UnsupportedOperationException(msg)
  }
}