package it.agilelab.bigdata.wasp.consumers.spark.plugins.mongo

import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.readers.{SparkBatchReader, SparkLegacyStreamingReader, SparkStructuredStreamingReader}
import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkBatchWriter, SparkLegacyStreamingWriter}
import it.agilelab.bigdata.wasp.repository.core.bl.{ConfigBL, DocumentBL}
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct.MongoDbProduct
import it.agilelab.bigdata.wasp.repository.core.db.WaspDB
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.configuration.ValidationRule
import it.agilelab.bigdata.wasp.models.{LegacyStreamingETLModel, ReaderModel, StreamingReaderModel, StructuredStreamingETLModel, WriterModel}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext


/**
  * Created by Agile Lab s.r.l. on 05/09/2017.
  */
class MongoConsumersSpark extends WaspConsumersSparkPlugin with Logging {
  var documentBL: DocumentBL = _

  override def datastoreProduct: DatastoreProduct = MongoDbProduct

  override def initialize(waspDB: WaspDB): Unit = {
    logger.info(s"Initialize the raw BL")
    documentBL = ConfigBL.documentBL
  }

  override def getValidationRules: Seq[ValidationRule] = Seq()

  override def getSparkLegacyStreamingWriter(ssc: StreamingContext,
                                             legacyStreamingETLModel: LegacyStreamingETLModel,
                                             writerModel: WriterModel): SparkLegacyStreamingWriter = {
    throw new UnsupportedOperationException("No legacy support for "  + this.getClass.getName)
  }
  
  override def getSparkLegacyStreamingReader(ssc: StreamingContext,
                                             legacyStreamingETLModel: LegacyStreamingETLModel,
                                             readerModel: ReaderModel): SparkLegacyStreamingReader = {
    throw new UnsupportedOperationException("No legacy support for "  + this.getClass.getName)
  }

  override def getSparkStructuredStreamingWriter(ss: SparkSession,
                                                 structuredStreamingETLModel: StructuredStreamingETLModel,
                                                 writerModel: WriterModel): MongoSparkStructuredStreamingWriter = {
    logger.info(s"Initialize $datastoreProduct spark structured streaming writer with this model: $writerModel")

    val documentModel = documentBL.getByName(writerModel.datastoreModelName)
      .getOrElse(throw new Exception(s"Cannot retrieve DocumentModel with name ${writerModel.datastoreModelName} "))

    new MongoSparkStructuredStreamingWriter(writerModel, documentModel)
  }
  
  override def getSparkStructuredStreamingReader(ss: SparkSession,
                                                 structuredStreamingETLModel: StructuredStreamingETLModel,
                                                 streamingReaderModel: StreamingReaderModel): SparkStructuredStreamingReader = {
    val msg = s"The datastore product $datastoreProduct is not a valid streaming source! Reader model $streamingReaderModel is not valid."
    logger.error(msg)
    throw new UnsupportedOperationException(msg)
  }

  override def getSparkBatchWriter(sc: SparkContext, writerModel: WriterModel): SparkBatchWriter = {

    val documentModel = documentBL.getByName(writerModel.datastoreModelName)
      .getOrElse(throw new Exception(s"Cannot retrieve DocumentModel with name ${writerModel.datastoreModelName} "))
    new MongoSparkBatchWriter(writerModel, documentModel)
  }

  override def getSparkBatchReader(sc: SparkContext, readerModel: ReaderModel): SparkBatchReader = {
    logger.info(s"Initialize HDFS reader with model $readerModel")
    val documentModel = documentBL.getByName(readerModel.datastoreModelName)
      .getOrElse(throw new Exception(s"Cannot retrieve DocumentModel with name ${readerModel.datastoreModelName} "))
    new MongoSparkBatchReader(readerModel, documentModel)
  }


}



