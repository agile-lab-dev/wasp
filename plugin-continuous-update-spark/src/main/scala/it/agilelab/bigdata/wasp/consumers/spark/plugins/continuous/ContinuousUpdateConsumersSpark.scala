package it.agilelab.bigdata.wasp.consumers.spark.plugins.continuous

import java.io.Serializable

import com.squareup.okhttp.OkHttpClient
import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.plugins.continuous.model.ContinuousUpdateModel
import it.agilelab.bigdata.wasp.consumers.spark.readers.{SparkBatchReader, SparkLegacyStreamingReader, SparkStructuredStreamingReader}
import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkBatchWriter, SparkLegacyStreamingWriter}
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.configuration.ValidationRule
import it.agilelab.bigdata.wasp.datastores.{DatastoreProduct, GenericProduct}
import it.agilelab.bigdata.wasp.models._
import it.agilelab.bigdata.wasp.repository.core.bl.{ConfigBL, GenericBL}
import it.agilelab.bigdata.wasp.repository.core.db.WaspDB
import net.liftweb.json.{DefaultFormats, parse}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

import scala.util.Try


/**
  * Created by Agile Lab s.r.l. on 23/03/2021.
  */
class ContinuousUpdateConsumersSparkPlugin extends WaspConsumersSparkPlugin with Logging {
  var genericBL: GenericBL = ConfigBL.genericBL

  override def datastoreProduct: DatastoreProduct = GenericProduct("continuousUpdate", Some("continuous"))

  override def initialize(waspDB: WaspDB): Unit = {
    logger.info(s"Initialize the Generic BL")
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
  ): ContinuousUpdateSparkStructuredStreamingWriter = {
    implicit val formats = DefaultFormats

    logger.info(s"Initialize the Continuous Update spark streaming writer with this model: $writerModel")
    val genericModel: GenericModel = Try(genericBL.getByName(writerModel.datastoreModelName) getOrElse (throw new NoSuchElementException("generic model not found: $name"))).get
    logger.info(s"Retrieved genericModel: ${genericModel.toString}")

    val continuousUpdateModel: ContinuousUpdateModel =
      if (genericModel.kind == "continuousUpdate") parse(genericModel.value.toJson).extract[ContinuousUpdateModel]
      else throw new IllegalArgumentException(s"""Expected value of GenericModel.kind is "continuousUpdate", found ${genericModel.value}""")
    logger.info(s"Successfully parsed genericModel: ${genericModel.toString} into ParallelWriteModel")


    val writer: DeltaLakeWriter = new DeltaLakeWriter(continuousUpdateModel, ss)
    new ContinuousUpdateSparkStructuredStreamingWriter(writer, continuousUpdateModel, ss)
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
