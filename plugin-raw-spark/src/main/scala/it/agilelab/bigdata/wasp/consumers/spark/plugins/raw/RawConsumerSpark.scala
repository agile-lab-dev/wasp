package it.agilelab.bigdata.wasp.consumers.spark.plugins.raw

import java.net.URI

import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumerSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.readers.StaticReader
import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkStreamingWriter, SparkWriter}
import it.agilelab.bigdata.wasp.core.bl.{RawBL, RawBLImp}
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.{RawModel, WriterModel}
import it.agilelab.bigdata.wasp.core.utils.WaspDB
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

/**
  * Created by Agile Lab s.r.l. on 05/09/2017.
  */
class RawConsumerSpark extends WaspConsumerSparkPlugin with Logging {
  var rawBL: RawBL = _

  override def initialize(waspDB: WaspDB): Unit = {
    logger.info(s"Initialize the RawBL")
    rawBL = new RawBLImp(waspDB)
  }

  override def getSparkStreamingWriter(ssc: StreamingContext, writerModel: WriterModel): SparkStreamingWriter = {
    logger.info(s"Initialize HDFSSparkStreamingWriter with this model: $writerModel")
    new HDFSSparkStreamingWriter(getModelAndChekHdfsSchema(writerModel.endpointId.getValue.toHexString), ssc)
  }

  override def getSparkStructuredStreamingWriter(ss: SparkSession, writerModel: WriterModel) = {
    logger.info(s"Initialize HDFSSparkStructuredStreamingWriter with this model: $writerModel")
    new HDFSSparkStructuredStreamingWriter(getModelAndChekHdfsSchema(writerModel.endpointId.getValue.toHexString), ss)
  }

  override def getSparkWriter(sc: SparkContext, writerModel: WriterModel): SparkWriter = {
    logger.info(s"Initialize HDFSSparkWriter with this model: $writerModel")
    new HDFSSparkWriter(getModelAndChekHdfsSchema(writerModel.endpointId.getValue.toHexString), sc)
  }

  override def getSparkReader(id: String, name: String): StaticReader = {
    logger.info(s"Initialize HDFSReader with this id: '$id' and name: '$name'")
    new HDFSReader(getModelAndChekHdfsSchema(id))
  }

  private def getModelAndChekHdfsSchema(id: String): RawModel = {
    // get the raw model using the provided id & bl
    val rawModelOpt = rawBL.getById(id)
    // if we found a model, try to return the correct reader
    if (rawModelOpt.isDefined) {
      val rawModel = rawModelOpt.get
      val scheme = new URI(rawModel.uri).getScheme
      scheme match {
        case "hdfs" => rawModel
        case _ => throw new Exception(s"Raw scheme not found $scheme, raw model: $rawModel")
      }
    } else {
      throw new Exception(s"Raw model not found: $id")
    }
  }

  override def pluginType: String = "raw"
}
