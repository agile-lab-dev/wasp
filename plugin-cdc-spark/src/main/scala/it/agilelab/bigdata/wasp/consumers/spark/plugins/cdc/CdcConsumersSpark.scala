package it.agilelab.bigdata.wasp.consumers.spark.plugins.cdc

import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.readers.{SparkBatchReader, SparkStructuredStreamingReader}
import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkBatchWriter, SparkStructuredStreamingWriter}
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.configuration.ValidationRule
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct.CdcProduct
import it.agilelab.bigdata.wasp.models._
import it.agilelab.bigdata.wasp.repository.core.bl.{CdcBL, ConfigBL}
import it.agilelab.bigdata.wasp.repository.core.db.WaspDB
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

class CdcConsumersSpark extends WaspConsumersSparkPlugin with Logging {

  var cdcBL: CdcBL = _

  override def initialize(waspDB: WaspDB): Unit = {
    logger.info(s"Initialized plugin for $datastoreProduct")
    cdcBL = ConfigBL.cdcBL
  }

  override def datastoreProduct: DatastoreProduct = CdcProduct

  override def getValidationRules: Seq[ValidationRule] = Seq()

  override def getSparkStructuredStreamingWriter(
      ss: SparkSession,
      structuredStreamingModel: StructuredStreamingETLModel,
      writerModel: WriterModel
  ): SparkStructuredStreamingWriter = {
    logger.info(s"Initialize Delta Lake spark structured streaming writer with this model: $writerModel")
    val model  = getModel(writerModel.datastoreModelName)
    val writer = new DeltaLakeWriter(model, ss)
    new CdcSparkStructuredStreamingWriter(writer, model, ss)
  }

  private def getModel(name: String): CdcModel = {

    val cdcModelOpt = cdcBL.getByName(name)

    if (cdcModelOpt.isDefined) {
      cdcModelOpt.get
    } else {
      throw new NoSuchElementException(s"Cdc model not found: $name")
    }
  }

  override def getSparkStructuredStreamingReader(
      ss: SparkSession,
      structuredStreamingETLModel: StructuredStreamingETLModel,
      streamingReaderModel: StreamingReaderModel
  ): SparkStructuredStreamingReader = {
    throw new UnsupportedOperationException("Method 'getSparkStructuredStreamingReader' not implemented.")
  }

  override def getSparkBatchWriter(sc: SparkContext, writerModel: WriterModel): SparkBatchWriter = {
    throw new UnsupportedOperationException("Batch Delta Writer not implemented. Please consider using a basic RawModel setting 'delta' as format.")
  }

  override def getSparkBatchReader(sc: SparkContext, readerModel: ReaderModel): SparkBatchReader = {
    logger.info(s"Initialize Delta reader with model $readerModel")
    throw new UnsupportedOperationException("Batch Delta Reader not implemented. Please consider using a basic RawModel setting 'delta' as format.")
  }

}
