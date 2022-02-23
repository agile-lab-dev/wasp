package it.agilelab.bigdata.wasp.consumers.spark.plugins.kafka

import it.agilelab.bigdata.wasp.consumers.spark.SparkSingletons
import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.readers.{SparkBatchReader, SparkStructuredStreamingReader}
import it.agilelab.bigdata.wasp.consumers.spark.writers._
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.configuration.ValidationRule
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct.KafkaProduct
import it.agilelab.bigdata.wasp.models._
import it.agilelab.bigdata.wasp.repository.core.bl.{ConfigBL, TopicBL}
import it.agilelab.bigdata.wasp.repository.core.db.WaspDB
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

class KafkaConsumersSpark extends WaspConsumersSparkPlugin with Logging {
  var topicBL: TopicBL = _

  override def datastoreProduct: DatastoreProduct = KafkaProduct

  override def initialize(waspDB: WaspDB): Unit = {
    logger.info(s"Initialize the kafka BL")
    topicBL = ConfigBL.topicBL
  }

  override def getValidationRules: Seq[ValidationRule] = Seq()

  override def getSparkStructuredStreamingWriter(ss: SparkSession,
                                                 structuredStreamingETLModel: StructuredStreamingETLModel,
                                                 writerModel: WriterModel): KafkaSparkStructuredStreamingWriter = {
    logger.info(s"Initialize the kafka spark structured streaming writer")
    logger.info(s"Topic: $topicBL")
    new KafkaSparkStructuredStreamingWriter(topicBL, writerModel.datastoreModelName, ss)
  }

  override def getSparkStructuredStreamingReader(ss: SparkSession,
                                                 structuredStreamingETLModel: StructuredStreamingETLModel,
                                                 streamingReaderModel: StreamingReaderModel): SparkStructuredStreamingReader = {
    logger.info(s"Returning object $KafkaSparkStructuredStreamingReader")
    // why is this an object? :/
    KafkaSparkStructuredStreamingReader
  }

  override def getSparkBatchWriter(sc: SparkContext, writerModel: WriterModel): SparkBatchWriter = {
    new KafkaBatchWriter(topicBL, writerModel.datastoreModelName, SparkSingletons.getSparkSession)
  }

  override def getSparkBatchReader(sc: SparkContext, readerModel: ReaderModel): SparkBatchReader = {
    val msg = s"The datastore product $datastoreProduct is not a valid batch source! Reader model $readerModel is not valid."
    logger.error(msg)
    throw new UnsupportedOperationException(msg)
  }
}