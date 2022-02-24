package it.agilelab.bigdata.wasp.consumers.spark.plugins.http


import it.agilelab.bigdata.wasp.models.HttpModel
import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.readers._
import it.agilelab.bigdata.wasp.consumers.spark.writers._
import it.agilelab.bigdata.wasp.core.models.configuration.ValidationRule
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct
import it.agilelab.bigdata.wasp.models._
import it.agilelab.bigdata.wasp.repository.core.bl.{ConfigBL, HttpBL}
import it.agilelab.bigdata.wasp.repository.core.db.WaspDB
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession


class HttpConsumersSparkPlugin extends WaspConsumersSparkPlugin {
  private val httpBL: HttpBL = ConfigBL.httpBl

  override def datastoreProduct: DatastoreProduct =
    it.agilelab.bigdata.wasp.datastores.DatastoreProduct.HttpProduct

  override def initialize(waspDB: WaspDB): Unit = ()

  override def getValidationRules: Seq[ValidationRule] = Seq.empty

  override def getSparkStructuredStreamingWriter(
    ss: SparkSession,
    structuredStreamingModel: StructuredStreamingETLModel,
    writerModel: WriterModel
  ): SparkStructuredStreamingWriter = {
    val httpModel: Option[HttpModel] = httpBL.getByName(writerModel.datastoreModelName)
    httpModel
      .map(new HttpWaspWriter(_))
      .getOrElse(throw new UnsupportedOperationException(s"Unknown http model: ${writerModel.datastoreModelName}"))
  }

  override def getSparkStructuredStreamingReader(
    ss: SparkSession,
    structuredStreamingETLModel: StructuredStreamingETLModel,
    streamingReaderModel: StreamingReaderModel
  ): SparkStructuredStreamingReader = throw new UnsupportedOperationException("Unimplemented HTTP streaming reader")

  override def getSparkBatchWriter(sc: SparkContext, writerModel: WriterModel): SparkBatchWriter =
    throw new UnsupportedOperationException("Unimplemented HTTP batch writer")

  override def getSparkBatchReader(sc: SparkContext, readerModel: ReaderModel): SparkBatchReader =
    throw new UnsupportedOperationException("Unimplemented HTTP batch reader")
}
