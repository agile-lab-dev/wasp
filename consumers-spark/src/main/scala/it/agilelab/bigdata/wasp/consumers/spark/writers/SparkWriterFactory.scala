package it.agilelab.bigdata.wasp.consumers.spark.writers

import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.core.bl.{IndexBL, KeyValueBL, RawBL, TopicBL}
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.{Datastores, WriterModel}
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext


trait SparkWriterFactory {
  def createSparkWriterStreaming(env: {val indexBL: IndexBL; val topicBL: TopicBL; val rawBL: RawBL; val keyValueBL: KeyValueBL}, ssc: StreamingContext, writerModel: WriterModel): Option[SparkLegacyStreamingWriter]

  def createSparkWriterStructuredStreaming(env: {val indexBL: IndexBL; val topicBL: TopicBL; val rawBL: RawBL; val keyValueBL: KeyValueBL}, ss: SparkSession, writerModel: WriterModel): Option[SparkStructuredStreamingWriter]

  def createSparkWriterBatch(env: {val indexBL: IndexBL; val rawBL: RawBL; val keyValueBL: KeyValueBL}, sc: SparkContext, writerModel: WriterModel): Option[SparkWriter]
}

case class SparkWriterFactoryDefault(plugins: Map[String, WaspConsumersSparkPlugin]) extends SparkWriterFactory with Logging {

  private val defaultDataStoreIndexed = ConfigManager.getWaspConfig.defaultIndexedDatastore

  override def createSparkWriterStreaming(env: {val topicBL: TopicBL; val indexBL: IndexBL; val rawBL: RawBL; val keyValueBL: KeyValueBL}, ssc: StreamingContext, writerModel: WriterModel): Option[SparkLegacyStreamingWriter] = {

    val writerType = writerModel.writerType.getActualProduct
    // Get the plugin, the index type does not exists anymore.
    // It was replace by the right data store like elastic or solr

    // Kafka isn't a plugin handle as exception.
    writerType match {
      case Datastores.kafkaProduct =>
        Some(new KafkaSparkLegacyStreamingWriter(env, ssc, writerModel.endpointId.getValue.toHexString))

      case _ =>
        val writerPlugin = plugins.get(writerType)

        if (writerPlugin.isDefined) {
          Some(writerPlugin.get.getSparkLegacyStreamingWriter(ssc, writerModel))
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

    val writerType = writerModel.writerType.getActualProduct
    // Get the plugin, the index type does not exists anymore.
    // It was replace by the right data store like elastic or solr

    // Kafka isn't a plugin handle as exception.
    writerType match {
      case Datastores.kafkaProduct =>
        Some(new KafkaSparkStructuredStreamingWriter(env, writerModel.endpointId.getValue.toHexString, ss))

      case _ =>
        val writerPlugin = plugins.get(writerType)
        if (writerPlugin.isDefined) {
          Some(writerPlugin.get.getSparkStructuredStreamingWriter(ss, writerModel))
        } else {
          logger.error(s"Invalid spark structured streaming writer type, writer model: $writerModel")
          None
        }
    }
  }

  override def createSparkWriterBatch(env: {val indexBL: IndexBL; val rawBL: RawBL; val keyValueBL: KeyValueBL}, sc: SparkContext, writerModel: WriterModel): Option[SparkWriter] = {
    val writerType = writerModel.writerType.getActualProduct
    
    val writerPlugin = plugins.get(writerType)
    if (writerPlugin.isDefined) {
      Some(writerPlugin.get.getSparkWriter(sc, writerModel))
    } else {
      logger.error(s"""Invalid Spark writer type: "$writerType" in writer model: $writerModel""")
      None
    }
  }
}