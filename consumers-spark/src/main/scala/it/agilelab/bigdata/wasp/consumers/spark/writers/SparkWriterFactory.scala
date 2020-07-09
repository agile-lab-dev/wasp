package it.agilelab.bigdata.wasp.consumers.spark.writers

import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.repository.core.bl.{IndexBL, KeyValueBL, RawBL, TopicBL}
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.models.{LegacyStreamingETLModel, StructuredStreamingETLModel, WriterModel}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

trait SparkWriterFactory {
  def createSparkWriterLegacyStreaming(env: {val indexBL: IndexBL; val topicBL: TopicBL; val rawBL: RawBL; val keyValueBL: KeyValueBL}, ssc: StreamingContext, legacyStreamingETLModel: LegacyStreamingETLModel, writerModel: WriterModel): Option[SparkLegacyStreamingWriter]
  def createSparkWriterStructuredStreaming(env: {val indexBL: IndexBL; val topicBL: TopicBL; val rawBL: RawBL; val keyValueBL: KeyValueBL}, ss: SparkSession, structuredStreamingETLModel: StructuredStreamingETLModel, writerModel: WriterModel): Option[SparkStructuredStreamingWriter]
  def createSparkWriterBatch(env: {val indexBL: IndexBL; val rawBL: RawBL; val keyValueBL: KeyValueBL}, sc: SparkContext, writerModel: WriterModel): Option[SparkBatchWriter]
}

case class SparkWriterFactoryDefault(plugins: Map[DatastoreProduct, WaspConsumersSparkPlugin]) extends SparkWriterFactory with Logging {

  override def createSparkWriterLegacyStreaming(env: {
    val topicBL: TopicBL
    val indexBL: IndexBL
    val rawBL: RawBL
    val keyValueBL: KeyValueBL
  }, ssc: StreamingContext, etlModel: LegacyStreamingETLModel, writerModel: WriterModel): Option[SparkLegacyStreamingWriter] = {

    val writerType = writerModel.datastoreProduct

    // Get the plugin
    val writerPlugin = plugins.get(writerType)

    if (writerPlugin.isDefined) {
      Some(writerPlugin.get.getSparkLegacyStreamingWriter(ssc, etlModel, writerModel))
    } else {
      logger.error(s"""Invalid spark streaming writer type: "$writerType" in writer model: $writerModel""")
      None
    }
  }

  override def createSparkWriterStructuredStreaming(env: {
    val rawBL: RawBL
    val keyValueBL: KeyValueBL
    val topicBL: TopicBL
    val indexBL: IndexBL
  }, ss: SparkSession, etlModel: StructuredStreamingETLModel, writerModel: WriterModel): Option[SparkStructuredStreamingWriter] = {
    val datastoreProduct = writerModel.datastoreProduct

    // Get the plugin
    val writerPlugin = plugins.get(datastoreProduct)
    if (writerPlugin.isDefined) {
      Some(writerPlugin.get.getSparkStructuredStreamingWriter(ss, etlModel, writerModel))
    } else {
      logger.error(s"""Invalid Spark Structured Streaming datastore product "$datastoreProduct" in writer model: $writerModel""")
      None
    }
  }

  override def createSparkWriterBatch(env: {
    val indexBL: IndexBL
    val rawBL: RawBL
    val keyValueBL: KeyValueBL
  }, sc: SparkContext, writerModel: WriterModel): Option[SparkBatchWriter] = {

    val writerType = writerModel.datastoreProduct

    // Get the plugin
    val writerPlugin = plugins.get(writerType)
    if (writerPlugin.isDefined) {
      Some(writerPlugin.get.getSparkBatchWriter(sc, writerModel))
    } else {
      logger.error(s"""Invalid spark writer type: "$writerType" in writer model: $writerModel""")
      None
    }
  }
}