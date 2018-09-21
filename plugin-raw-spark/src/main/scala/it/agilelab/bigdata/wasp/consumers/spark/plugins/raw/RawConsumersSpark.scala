package it.agilelab.bigdata.wasp.consumers.spark.plugins.raw

import java.net.URI

import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.readers.{SparkBatchReader, SparkLegacyStreamingReader, SparkStructuredStreamingReader}
import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkBatchWriter, SparkLegacyStreamingWriter}
import it.agilelab.bigdata.wasp.core.bl.{RawBL, RawBLImp}
import it.agilelab.bigdata.wasp.core.datastores.DatastoreProduct
import it.agilelab.bigdata.wasp.core.datastores.DatastoreProduct.RawProduct
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.configuration.ValidationRule
import it.agilelab.bigdata.wasp.core.models._
import it.agilelab.bigdata.wasp.core.utils.WaspDB
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

/**
  * Created by Agile Lab s.r.l. on 05/09/2017.
  */
class RawConsumersSpark extends WaspConsumersSparkPlugin with Logging {
  var rawBL: RawBL = _

  override def datastoreProduct: DatastoreProduct = RawProduct

  override def initialize(waspDB: WaspDB): Unit = {
    logger.info(s"Initialize the raw BL")
    rawBL = new RawBLImp(waspDB)
  }

  override def getValidationRules: Seq[ValidationRule] = Seq()

  override def getSparkLegacyStreamingWriter(ssc: StreamingContext,
                                             legacyStreamingETLModel: LegacyStreamingETLModel,
                                             writerModel: WriterModel): SparkLegacyStreamingWriter = {
    logger.info(s"Initialize the HDFS spark streaming writer with this model: $writerModel")
    new RawSparkLegacyStreamingWriter(getModelAndCheckHdfsSchema(writerModel.datastoreModelName), ssc)
  }
  
  override def getSparkLegacyStreamingReader(ssc: StreamingContext,
                                             legacyStreamingETLModel: LegacyStreamingETLModel,
                                             readerModel: ReaderModel): SparkLegacyStreamingReader = {
    val msg = s"The datastore product $datastoreProduct is not a valid streaming source! Reader model $readerModel is not valid."
    logger.error(msg)
    throw new UnsupportedOperationException(msg)
  }

  override def getSparkStructuredStreamingWriter(ss: SparkSession,
                                                 structuredStreamingETLModel: StructuredStreamingETLModel,
                                                 writerModel: WriterModel): RawSparkStructuredStreamingWriter = {
    logger.info(s"Initialize HDFS spark structured streaming writer with this model: $writerModel")
    new RawSparkStructuredStreamingWriter(getModelAndCheckHdfsSchema(writerModel.datastoreModelName), ss)
  }
  
  override def getSparkStructuredStreamingReader(ss: SparkSession,
                                                 structuredStreamingETLModel: StructuredStreamingETLModel,
                                                 readerModel: ReaderModel): SparkStructuredStreamingReader = {
    val msg = s"The datastore product $datastoreProduct is not a valid streaming source! Reader model $readerModel is not valid."
    logger.error(msg)
    throw new UnsupportedOperationException(msg)
  }

  override def getSparkBatchWriter(sc: SparkContext, writerModel: WriterModel): SparkBatchWriter = {
    logger.info(s"Initialize HDFS spark batch writer with this model: $writerModel")
    new RawSparkBatchWriter(getModelAndCheckHdfsSchema(writerModel.datastoreModelName), sc)
  }

  override def getSparkBatchReader(sc: SparkContext, readerModel: ReaderModel): SparkBatchReader = {
    logger.info(s"Initialize HDFS reader with model $readerModel")
    new RawSparkBatchReader(getModelAndCheckHdfsSchema(readerModel.name))
  }

  private def getModelAndCheckHdfsSchema(name: String): RawModel = {
    // get the raw model using the provided id & bl
    val rawModelOpt = rawBL.getByName(name)
    // if we found a model, try to return the correct reader
    if (rawModelOpt.isDefined) {
      val rawModel = rawModelOpt.get
      val scheme = new URI(rawModel.uri).getScheme
      scheme match {
        case "hdfs" => rawModel
        case _ => throw new Exception(s"Raw scheme not found $scheme, raw model: $rawModel")
      }
    } else {
      throw new Exception(s"Raw model not found: $name")
    }
  }
}
