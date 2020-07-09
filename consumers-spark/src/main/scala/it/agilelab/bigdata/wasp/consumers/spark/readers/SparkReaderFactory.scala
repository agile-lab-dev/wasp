package it.agilelab.bigdata.wasp.consumers.spark.readers

import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.repository.core.bl._
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.models.{LegacyStreamingETLModel, ReaderModel, StreamingReaderModel, StructuredStreamingETLModel}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext


trait SparkReaderFactory {
  def createSparkLegacyStreamingReader(env: DatastoreModelBLs,
                                       ssc: StreamingContext,
                                       legacyStreamingETLModel: LegacyStreamingETLModel,
                                       readerModel: ReaderModel): Option[SparkLegacyStreamingReader]
  def createSparkStructuredStreamingReader(env: DatastoreModelBLs,
                                           ss: SparkSession,
                                           structuredStreamingETLModel: StructuredStreamingETLModel,
                                           streamingReaderModel: StreamingReaderModel): Option[SparkStructuredStreamingReader]
  def createSparkBatchReader(env: DatastoreModelBLs,
                             sc: SparkContext,
                             readerModel: ReaderModel): Option[SparkBatchReader]
}

class PluginBasedSparkReaderFactory(plugins: Map[DatastoreProduct, WaspConsumersSparkPlugin])
    extends SparkReaderFactory
    with Logging {

  override def createSparkLegacyStreamingReader(env: DatastoreModelBLs,
                                                ssc: StreamingContext,
                                                legacyStreamingETLModel: LegacyStreamingETLModel,
                                                readerModel: ReaderModel): Option[SparkLegacyStreamingReader] = {
    lookupPluginForReaderModel(readerModel)
      .map(_.getSparkLegacyStreamingReader(ssc, legacyStreamingETLModel, readerModel))
  }

  override def createSparkStructuredStreamingReader(env: DatastoreModelBLs,
                                                    ss: SparkSession,
                                                    structuredStreamingETLModel: StructuredStreamingETLModel,
                                                    streamingReaderModel: StreamingReaderModel)
                                                   : Option[SparkStructuredStreamingReader] = {
    lookupPluginForStreamingReaderModel(streamingReaderModel)
      .map(_.getSparkStructuredStreamingReader(ss, structuredStreamingETLModel, streamingReaderModel))
  }
  
  override def createSparkBatchReader(env: DatastoreModelBLs,
                                      sc: SparkContext,
                                      readerModel: ReaderModel): Option[SparkBatchReader] = {
    lookupPluginForReaderModel(readerModel).map(_.getSparkBatchReader(sc, readerModel))
  }
  
  private def lookupPluginForReaderModel(readerModel: ReaderModel): Option[WaspConsumersSparkPlugin] = {
    val datastoreProduct = readerModel.datastoreProduct
    val plugin = plugins.get(datastoreProduct)
    if (plugin.isDefined) {
      plugin
    } else {
      logger.error(s"No plugin found for datastore: $datastoreProduct used by reader model: $readerModel")
      None
    }
  }
  
  private def lookupPluginForStreamingReaderModel(streamingReaderModel: StreamingReaderModel)
                                                 : Option[WaspConsumersSparkPlugin] = {
    val datastoreProduct = streamingReaderModel.datastoreProduct
    val plugin = plugins.get(datastoreProduct)
    if (plugin.isDefined) {
      plugin
    } else {
      logger.error(s"No plugin found for datastore: $datastoreProduct used by streaming reader model: $streamingReaderModel")
      None
    }
  }
}