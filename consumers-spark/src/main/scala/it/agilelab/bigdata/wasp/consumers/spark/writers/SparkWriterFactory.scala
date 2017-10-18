package it.agilelab.bigdata.wasp.consumers.spark.writers

import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumerSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.utils.Utils
import it.agilelab.bigdata.wasp.core.bl.{IndexBL, KeyValueBL, RawBL, TopicBL}
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.{Datastores, WriterModel}
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext


trait SparkWriterFactory {
  def createSparkWriterStreaming(env: {val indexBL: IndexBL; val topicBL: TopicBL; val rawBL: RawBL; val keyValueBL: KeyValueBL}, ssc: StreamingContext, writerModel: WriterModel): Option[SparkStreamingWriter]

  def createSparkWriterStructuredStreaming(env: {val indexBL: IndexBL; val topicBL: TopicBL; val rawBL: RawBL; val keyValueBL: KeyValueBL}, ss: SparkSession, writerModel: WriterModel): Option[SparkStructuredStreamingWriter]

  def createSparkWriterBatch(env: {val indexBL: IndexBL; val rawBL: RawBL; val keyValueBL: KeyValueBL}, sc: SparkContext, writerModel: WriterModel): Option[SparkWriter]
}

case class SparkWriterFactoryDefault(plugins: Map[String, WaspConsumerSparkPlugin]) extends SparkWriterFactory with Logging {

  private val defaultDataStoreIndexed = ConfigManager.getWaspConfig.defaultIndexedDatastore

  override def createSparkWriterStreaming(env: {val topicBL: TopicBL; val indexBL: IndexBL; val rawBL: RawBL; val keyValueBL: KeyValueBL}, ssc: StreamingContext, writerModel: WriterModel): Option[SparkStreamingWriter] = {

    val writerType = writerModel.writerType.getActualProduct
    // Get the plugin, the index type does not exists anymore.
    // It was replace by the right data store like elastic or solr

    // Kafka isn't a plugin handle as exception.
    writerType match {
      case Datastores.kafkaProduct =>
        Some(new KafkaSparkStreamingWriter(env, ssc, writerModel.endpointId.getValue.toHexString))

      case _ =>
        val backCompatibilityWriteType = Utils.getIndexType(writerType, defaultDataStoreIndexed)
        logger.info(s"Get SparkWriterStreaming plugin $backCompatibilityWriteType before was $writerType, plugin map: $plugins")

        val writerPlugin = plugins.get(backCompatibilityWriteType)
        if (writerPlugin.isDefined) {
          Some(writerPlugin.get.getSparkStreamingWriter(ssc, writerModel))
        } else {
          logger.error(s"Invalid spark streaming writer type, writer model: $writerModel")
          None
        }
    }


  }

  override def createSparkWriterStructuredStreaming(env: {
    val rawBL: RawBL

    val keyValueBL: KeyValueBL

    val topicBL: TopicBL

    val indexBL: IndexBL
  }, ss: SparkSession, writerModel: WriterModel) = {

    val writerType = writerModel.writerType.product.getOrElse(writerModel.writerType.category)
    // Get the plugin, the index type does not exists anymore.
    // It was replace by the right data store like elastic or solr
    val backCompatibilityWriteType = Utils.getIndexType(writerType, defaultDataStoreIndexed)
    logger.info(s"Get SparkWriterStructuredStreaming plugin $backCompatibilityWriteType before was $writerType, plugin map: $plugins")

    val writerPlugin = plugins.get(backCompatibilityWriteType)
    if (writerPlugin.isDefined) {
      Some(writerPlugin.get.getSparkStructuredStreamingWriter(ss, writerModel))
    } else {
      logger.error(s"Invalid spark structured streaming writer type, writer model: $writerModel")
      None
    }

  }

  override def createSparkWriterBatch(env: {val indexBL: IndexBL; val rawBL: RawBL; val keyValueBL: KeyValueBL}, sc: SparkContext, writerModel: WriterModel): Option[SparkWriter] = {
    val writerType = writerModel.writerType.product.getOrElse(writerModel.writerType.category)
    // Get the plugin, the index type does not exists anymore.
    // It was replace by the right data store like elastic or solr
    val backCompatibilityWriteType = Utils.getIndexType(writerType, defaultDataStoreIndexed)
    logger.info(s"Get SparkWriterStreaming plugin $backCompatibilityWriteType before was $writerType, plugin map: $plugins")

    val writerPlugin = plugins.get(backCompatibilityWriteType)
    if (writerPlugin.isDefined) {
      Some(writerPlugin.get.getSparkWriter(sc, writerModel))
    } else {
      logger.error(s"Invalid spark writer type, writer model: $writerModel")
      None
    }
  }
}