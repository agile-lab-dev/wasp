package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel

import com.squareup.okhttp.OkHttpClient
import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.model.ParallelWriteModelParser.parseParallelWriteModel
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.utils.{DataCatalogService, GlueDataCatalogService}
import it.agilelab.bigdata.wasp.consumers.spark.readers.{SparkBatchReader, SparkStructuredStreamingReader}
import it.agilelab.bigdata.wasp.consumers.spark.writers.SparkBatchWriter
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.configuration.ValidationRule
import it.agilelab.bigdata.wasp.datastores.{DatastoreProduct, GenericProduct}
import it.agilelab.bigdata.wasp.models._
import it.agilelab.bigdata.wasp.repository.core.bl.{ConfigBL, GenericBL}
import it.agilelab.bigdata.wasp.repository.core.db.WaspDB
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import java.io.Serializable
import scala.util.Try


/**
  * Created by Agile Lab s.r.l. on 23/03/2021.
  */
class ParallelWriteConsumersSparkPlugin extends WaspConsumersSparkPlugin with Logging {
  var genericBL: GenericBL = ConfigBL.genericBL
  val okHttpClient: OkHttpClient = new OkHttpClient() with Serializable

  override def datastoreProduct: DatastoreProduct = GenericProduct("parallelWrite", None)

  override def initialize(waspDB: WaspDB): Unit = {
    logger.info(s"Initialize the Generic BL")
  }

  override def getValidationRules: Seq[ValidationRule] = Seq()

  override def getSparkStructuredStreamingWriter(
      ss: SparkSession,
      structuredStreamingETLModel: StructuredStreamingETLModel,
      writerModel: WriterModel
  ): ParallelWriteSparkStructuredStreamingWriter = {

    logger.info(s"Initialize the Parallel Write spark streaming writer with this model: $writerModel")
    val genericModel: GenericModel = Try(genericBL.getByName(writerModel.datastoreModelName) getOrElse (throw new RuntimeException("generic model not found: $name"))).get
    logger.info(s"Retrieved genericModel: ${genericModel.toString}")

    new ParallelWriteSparkStructuredStreamingWriter(parseParallelWriteModel(genericModel), GlueDataCatalogService)
  }


  override def getSparkStructuredStreamingReader(
      ss: SparkSession,
      structuredStreamingETLModel: StructuredStreamingETLModel,
      streamingReaderModel: StreamingReaderModel
  ): SparkStructuredStreamingReader = {
    val msg =
      s"The datastore product $datastoreProduct is not a valid streaming source! Reader model $streamingReaderModel is not valid."
    logger.error(msg)
    throw new UnsupportedOperationException(msg)
  }

  override def getSparkBatchWriter(sc: SparkContext, writerModel: WriterModel): SparkBatchWriter = {
    throw new UnsupportedOperationException("Unimplemented Parallel Write legacy streaming writer")

  }

  override def getSparkBatchReader(sc: SparkContext, readerModel: ReaderModel): SparkBatchReader = {
    throw new UnsupportedOperationException("Unimplemented Parallel Write legacy streaming writer")

  }

}
