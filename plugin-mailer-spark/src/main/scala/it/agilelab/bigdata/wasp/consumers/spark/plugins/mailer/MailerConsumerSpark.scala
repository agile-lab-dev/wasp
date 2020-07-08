package it.agilelab.bigdata.wasp.consumers.spark.plugins.mailer

import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.readers.{SparkBatchReader, SparkLegacyStreamingReader, SparkStructuredStreamingReader}
import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkBatchWriter, SparkLegacyStreamingWriter}
import it.agilelab.bigdata.wasp.core.bl.{ConfigBL, IndexBL}
import it.agilelab.bigdata.wasp.core.datastores.DatastoreProduct
import it.agilelab.bigdata.wasp.core.db.WaspDB
import it.agilelab.bigdata.wasp.core.models._
import it.agilelab.bigdata.wasp.core.models.configuration.ValidationRule
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

class MailerConsumerSpark extends WaspConsumersSparkPlugin {
  var indexBL: IndexBL = _

  override def datastoreProduct: DatastoreProduct = DatastoreProduct.WebMailProduct

  override def initialize(waspDB: WaspDB): Unit = {
    indexBL = ConfigBL.indexBL
  }

  override def getValidationRules: Seq[ValidationRule] = Seq()

  override def getSparkLegacyStreamingWriter(ssc: StreamingContext,
                                             legacyStreamingETLModel: LegacyStreamingETLModel,
                                             writerModel: WriterModel): SparkLegacyStreamingWriter = {
    throw new UnsupportedOperationException(s"Unsupported: spark legacy streaming mail writer")
  }

  override def getSparkLegacyStreamingReader(ssc: StreamingContext,
                                             legacyStreamingETLModel: LegacyStreamingETLModel,
                                             readerModel: ReaderModel): SparkLegacyStreamingReader =
    throw new UnsupportedOperationException("Unsupported: spark legacy mail reader")

  override def getSparkStructuredStreamingWriter(ss: SparkSession,
                                                 structuredStreamingETLModel: StructuredStreamingETLModel,
                                                 writerModel: WriterModel): MailWriter = {
    //logger.info(s"Initialize the mail spark structured streaming writer with this writer model endpointName '${writerModel.datastoreModelName}'")
    new MailWriter(writerModel.options)
  }

  override def getSparkStructuredStreamingReader(ss: SparkSession,
                                                 structuredStreamingETLModel: StructuredStreamingETLModel,
                                                 streamingReaderModel: StreamingReaderModel): SparkStructuredStreamingReader =
    throw new UnsupportedOperationException("Unsupported: spark structured streaming reader")


  override def getSparkBatchWriter(sc: SparkContext, writerModel: WriterModel): SparkBatchWriter =
    throw new UnsupportedOperationException("Unsupported: spark batch writer")

  override def getSparkBatchReader(sc: SparkContext, readerModel: ReaderModel): SparkBatchReader =
    throw new UnsupportedOperationException("Unsupported: spark batch reader")

}