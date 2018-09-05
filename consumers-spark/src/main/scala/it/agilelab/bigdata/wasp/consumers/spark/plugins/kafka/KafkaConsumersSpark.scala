package it.agilelab.bigdata.wasp.consumers.spark.plugins.kafka

import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.readers.SparkReader
import it.agilelab.bigdata.wasp.consumers.spark.writers._
import it.agilelab.bigdata.wasp.core.bl.{TopicBL, TopicBLImp}
import it.agilelab.bigdata.wasp.core.datastores.DatastoreProduct.KafkaProduct
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.configuration.ValidationRule
import it.agilelab.bigdata.wasp.core.models.WriterModel
import it.agilelab.bigdata.wasp.core.utils.WaspDB
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

class KafkaConsumersSpark extends WaspConsumersSparkPlugin with Logging {
  var topicBL: TopicBL = _

  override def initialize(waspDB: WaspDB): Unit = {
    logger.info(s"Initialize the kafka BL")
    topicBL = new TopicBLImp(waspDB)
  }

  override def getSparkLegacyStreamingWriter(ssc: StreamingContext, writerModel: WriterModel): SparkLegacyStreamingWriter = {
    logger.info(s"Initialize the kafka spark streaming writer")
    new KafkaSparkLegacyStreamingWriter(topicBL, ssc, writerModel.datastoreModelName)
  }

  override def getSparkStructuredStreamingWriter(ss: SparkSession, writerModel: WriterModel) = {
    logger.info(s"Initialize the kafka spark structured streaming writer")
    logger.info(s"Topic: $topicBL")
    new KafkaSparkStructuredStreamingWriter(topicBL, writerModel.datastoreModelName, ss)
  }

  override def getSparkWriter(sc: SparkContext, writerModel: WriterModel): SparkWriter = {
    // BatchJobActor cannot use kafka WriterModel
    val msg = s"Invalid spark batch writer type: kafka in writer model: $writerModel"
    logger.error(msg)
    throw new UnsupportedOperationException(msg)
  }

  override def getSparkReader(id: String, name: String): SparkReader = {
    // BatchJobActor cannot use kafka ReaderModel
    val msg = s"Invalid spark reader type: kafka - name: $name"
    logger.error(msg)
    throw new UnsupportedOperationException(msg)
  }

  override def pluginType: String = KafkaProduct.getActualProduct

  override def getValidationRules: Seq[ValidationRule] = Seq()
}