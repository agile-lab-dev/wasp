package it.agilelab.bigdata.wasp.consumers.spark.plugins.raw

import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.readers.{SparkBatchReader, SparkStructuredStreamingReader}
import it.agilelab.bigdata.wasp.consumers.spark.writers.SparkBatchWriter
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.configuration.ValidationRule
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct.RawProduct
import it.agilelab.bigdata.wasp.models._
import it.agilelab.bigdata.wasp.repository.core.bl.{ConfigBL, RawBL}
import it.agilelab.bigdata.wasp.repository.core.db.WaspDB
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.sources.DataSourceRegister

import java.net.URI
import scala.util.Try

object RawConsumersSpark {
  private def safeGetShortName(className: String): Option[String] = Try {
    getClass.getClassLoader.loadClass(className)
      .asInstanceOf[Class[DataSourceRegister]]
  }.toOption.flatMap(safeGetShortNameC)

  private def safeGetShortNameC[T <: DataSourceRegister](cls: Class[T]): Option[String] = Try {
    cls.getDeclaredConstructor().newInstance().shortName
  }.toOption

  // this list is only used to issue a warning log, so if we cannot get some short names,
  // we swallow the exception
  private val WARNING_FORMATS: List[String] = List(
    safeGetShortName("org.apache.spark.sql.avro.AvroFileFormat"),
    safeGetShortNameC(classOf[CSVFileFormat]),
    safeGetShortNameC(classOf[JsonFileFormat]),
    safeGetShortName("org.apache.spark.sql.execution.datasources.orc.OrcFileFormat"),
    safeGetShortNameC(classOf[ParquetFileFormat])
  ).flatten
}

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
    val model = getModelAndCheckSchema(streamingReaderModel.datastoreModelName)
    if (RawConsumersSpark.WARNING_FORMATS.contains(model.options.format)) {
      logger.warn(s"Format ${model.options.format} is discouraged as a Streaming input")
    }
    new RawSparkStructuredStreamingReader(model)
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
      rawModel <- Try(rawBL.getByName(name) getOrElse (throw new RuntimeException(s"Raw model not found: $name")))
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
