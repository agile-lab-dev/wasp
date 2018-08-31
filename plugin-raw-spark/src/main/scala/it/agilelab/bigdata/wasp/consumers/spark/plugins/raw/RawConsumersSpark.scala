package it.agilelab.bigdata.wasp.consumers.spark.plugins.raw

import java.net.URI

import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.readers.SparkReader
import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkLegacyStreamingWriter, SparkWriter}
import it.agilelab.bigdata.wasp.core.bl.{RawBL, RawBLImp}
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.configuration.ValidationRule
import it.agilelab.bigdata.wasp.core.models.{Datastores, RawModel, WriterModel}
import it.agilelab.bigdata.wasp.core.utils.WaspDB
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

/**
  * Created by Agile Lab s.r.l. on 05/09/2017.
  */
class RawConsumersSpark extends WaspConsumersSparkPlugin with Logging {
  var rawBL: RawBL = _

  override def initialize(waspDB: WaspDB): Unit = {
    logger.info(s"Initialize the raw BL")
    rawBL = new RawBLImp(waspDB)
  }

  override def getSparkLegacyStreamingWriter(ssc: StreamingContext, writerModel: WriterModel): SparkLegacyStreamingWriter = {
    logger.info(s"Initialize the HDFS spark streaming writer with this model: $writerModel")
    new RawSparkLegacyStreamingWriter(getModelAndChekHdfsSchema(writerModel.datastoreModelName.get), ssc)
  }

  override def getSparkStructuredStreamingWriter(ss: SparkSession, writerModel: WriterModel) = {
    logger.info(s"Initialize HDFS spark structured streaming writer with this model: $writerModel")
    new RawSparkStructuredStreamingWriter(getModelAndChekHdfsSchema(writerModel.datastoreModelName.get), ss)
  }

  override def getSparkWriter(sc: SparkContext, writerModel: WriterModel): SparkWriter = {
    logger.info(s"Initialize HDFS spark batch writer with this model: $writerModel")
    new RawSparkWriter(getModelAndChekHdfsSchema(writerModel.datastoreModelName.get), sc)
  }

  override def getSparkReader(id: String, name: String): SparkReader = {
    logger.info(s"Initialize HDFS reader with this id: '$id' and name: '$name'")
    new RawSparkReader(getModelAndChekHdfsSchema(id))
  }

  private def getModelAndChekHdfsSchema(name: String): RawModel = {
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

  override def pluginType: String = Datastores.rawProduct

  override def getValidationRules: Seq[ValidationRule] = Seq()
}
