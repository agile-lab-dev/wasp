package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel

import com.squareup.okhttp.OkHttpClient

import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.readers.{SparkBatchReader, SparkLegacyStreamingReader, SparkStructuredStreamingReader}
import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkBatchWriter, SparkLegacyStreamingWriter}
import it.agilelab.bigdata.wasp.repository.core.bl.{ConfigBL, GenericBL}
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct
import it.agilelab.bigdata.wasp.repository.core.db.WaspDB
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.configuration.ValidationRule
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct.genericProduct
import it.agilelab.bigdata.wasp.models.{GenericModel, LegacyStreamingETLModel, ReaderModel, StreamingReaderModel, StructuredStreamingETLModel, WriterModel}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

import java.io.Serializable
import scala.util.Try


/**
  * Created by Agile Lab s.r.l. on 23/03/2021.
  */
class ParallelWriteConsumersSparkPlugin extends WaspConsumersSparkPlugin with Logging {
  var genericBL: GenericBL = ConfigBL.genericBL
  val okHttpClient: OkHttpClient = new OkHttpClient() with Serializable

  override def datastoreProduct: DatastoreProduct = genericProduct

  override def initialize(waspDB: WaspDB): Unit = {
    logger.info(s"Initialize the Generic BL")
    genericBL
    okHttpClient
  }

  override def getValidationRules: Seq[ValidationRule] = Seq()

  override def getSparkLegacyStreamingWriter(
      ssc: StreamingContext,
      legacyStreamingETLModel: LegacyStreamingETLModel,
      writerModel: WriterModel
  ): SparkLegacyStreamingWriter = {
    throw new UnsupportedOperationException("Unimplemented Parallel Write legacy streaming writer")
  }

  override def getSparkLegacyStreamingReader(
      ssc: StreamingContext,
      legacyStreamingETLModel: LegacyStreamingETLModel,
      readerModel: ReaderModel
  ): SparkLegacyStreamingReader = {
    val msg =
      s"The datastore product $datastoreProduct is not a valid streaming source! Reader model $readerModel is not valid."
    logger.error(msg)
    throw new UnsupportedOperationException(msg)
  }

  override def getSparkStructuredStreamingWriter(
      ss: SparkSession,
      structuredStreamingETLModel: StructuredStreamingETLModel,
      writerModel: WriterModel
  ): ParallelWriteSparkStructuredStreamingWriter = {
    logger.info(s"Initialize the Parallel Write spark streaming writer with this model: $writerModel")
    val genericModel: GenericModel = Try(genericBL.getByName(writerModel.datastoreModelName) getOrElse (throw new RuntimeException("generic model not found: $name"))).get
    logger.info(s"Retrieved genericModel: ${genericModel.toString}")
    new ParallelWriteSparkStructuredStreamingWriter(genericModel, ss)
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
