package it.agilelab.bigdata.wasp.consumers.spark.plugins.kafka

import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.readers.SparkReader
import it.agilelab.bigdata.wasp.consumers.spark.writers._
import it.agilelab.bigdata.wasp.core.bl.{TopicBL, TopicBLImp}
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.{Datastores, WriterModel}
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
    new KafkaSparkLegacyStreamingWriter(topicBL, ssc, writerModel.endpointId.get.getValue.toHexString)
  }

  override def getSparkStructuredStreamingWriter(ss: SparkSession, writerModel: WriterModel) = {
    logger.info(s"Initialize the kafka spark structured streaming writer")
    logger.info(s"Topic: $topicBL")
    new KafkaSparkStructuredStreamingWriter(topicBL, writerModel.endpointId.get.getValue.toHexString, ss)
  }

  override def getSparkWriter(sc: SparkContext, writerModel: WriterModel): SparkWriter = {
    // BatchJobActor cannot use kafka WriterModel
    val error = s"Invalid spark batch writer type: kafka in writer model: $writerModel"
    logger.error(error)
    throw new UnsupportedOperationException(error)
  }

  override def getSparkReader(id: String, name: String): SparkReader = {
    // BatchJobActor cannot use kafka ReaderModel
    val error = s"Invalid spark reader type: kafka - name: $name"
    logger.error(error)
    throw new UnsupportedOperationException(error)
  }

  override def pluginType: String = Datastores.kafkaProduct
}