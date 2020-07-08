package it.agilelab.bigdata.wasp.consumers.spark.plugins.raw

import java.net.URI

import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.readers.{SparkBatchReader, SparkLegacyStreamingReader, SparkStructuredStreamingReader}
import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkBatchWriter, SparkLegacyStreamingWriter}
import it.agilelab.bigdata.wasp.core.bl.{ConfigBL, RawBL}
import it.agilelab.bigdata.wasp.core.datastores.DatastoreProduct
import it.agilelab.bigdata.wasp.core.datastores.DatastoreProduct.RawProduct
import it.agilelab.bigdata.wasp.core.db.WaspDB
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.configuration.ValidationRule
import it.agilelab.bigdata.wasp.core.models._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

import scala.util.Try

/**
  * Created by Agile Lab s.r.l. on 05/09/2017.
  */
class RawConsumersSpark extends WaspConsumersSparkPlugin with Logging {
  var rawBL: RawBL = _

  override def datastoreProduct: DatastoreProduct = RawProduct

  override def initialize(waspDB: WaspDB): Unit = {
    logger.info(s"Initialize the raw BL")
    rawBL = ConfigBL.rawBL
  }

  override def getValidationRules: Seq[ValidationRule] = Seq()

  override def getSparkLegacyStreamingWriter(
      ssc: StreamingContext,
      legacyStreamingETLModel: LegacyStreamingETLModel,
      writerModel: WriterModel
  ): SparkLegacyStreamingWriter = {
    logger.info(s"Initialize the Raw spark streaming writer with this model: $writerModel")
    new RawSparkLegacyStreamingWriter(getModelAndCheckSchema(writerModel.datastoreModelName), ssc)
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
  ): RawSparkStructuredStreamingWriter = {
    logger.info(s"Initialize Raw spark structured streaming writer with this model: $writerModel")
    new RawSparkStructuredStreamingWriter(getModelAndCheckSchema(writerModel.datastoreModelName), ss)
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
    logger.info(s"Initialize Raw spark batch writer with this model: $writerModel")
    new RawSparkBatchWriter(getModelAndCheckSchema(writerModel.datastoreModelName), sc)
  }

  override def getSparkBatchReader(sc: SparkContext, readerModel: ReaderModel): SparkBatchReader = {
    logger.info(s"Initialize Raw reader with model $readerModel")
    new RawSparkBatchReader(getModelAndCheckSchema(readerModel.name))
  }

  private def getModelAndCheckSchema(name: String): RawModel = {
    // get the raw model using the provided id & bl
    for {
      rawModel <- Try(rawBL.getByName(name) getOrElse (throw new RuntimeException("Raw model not found: $name")))
      uri      <- Try(new URI(rawModel.uri))
    } yield {
      Option(uri.getScheme) match {
        case None =>
          logger.warn(s"No scheme specified for model $name, it will use writer default FS")
        case Some(scheme) =>
          logger.debug(s"RawModel uri has scheme $scheme")
      }
      rawModel
    }
  }.get
}
