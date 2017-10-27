package it.agilelab.bigdata.wasp.consumers.spark.plugins.hbase

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, Props}
import akka.util.Timeout
import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumerSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.readers.StaticReader
import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkStreamingWriter, SparkWriter}
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.WaspSystem.waspConfig
import it.agilelab.bigdata.wasp.core.bl.{KeyValueBL, KeyValueBLImp}
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.WriterModel
import it.agilelab.bigdata.wasp.core.utils.WaspDB
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext


/**
  * Created by Agile Lab s.r.l. on 05/09/2017.
  */
class HBaseConsumerSpark extends WaspConsumerSparkPlugin with Logging {
  var keyValueBL: KeyValueBL = _
  var hbaseAdminActor_ : ActorRef = _

  override def initialize(waspDB: WaspDB): Unit = {
    logger.info("Initialize the index BL")
    keyValueBL = new KeyValueBLImp(waspDB)
    logger.info(s"Initialize the elastic admin actor with this name ${HBaseAdminActor.name}")
    hbaseAdminActor_ = WaspSystem.actorSystem
      .actorOf(Props(new HBaseAdminActor), HBaseAdminActor.name)
    // services timeout, used below
    val servicesTimeoutMillis = waspConfig.servicesTimeoutMillis
    // implicit timeout used below
    implicit val timeout: Timeout = new Timeout(servicesTimeoutMillis, TimeUnit.MILLISECONDS)
    startupHBase(servicesTimeoutMillis)
  }

  override def getSparkStreamingWriter(ssc: StreamingContext, writerModel: WriterModel): SparkStreamingWriter = {
    logger.info(s"Initialize the elastic spark streaming writer with this writer model id '${writerModel.endpointId.getValue.toHexString}'")
    HBaseWriter.createSparkStreamingWriter(keyValueBL, ssc, writerModel.endpointId.getValue.toHexString)
  }

  override def getSparkWriter(sc: SparkContext, writerModel: WriterModel): SparkWriter = {
    logger.info(s"Initialize the elastic spark batch writer with this writer model id '${writerModel.endpointId.getValue.toHexString}'")
    HBaseWriter.createSparkWriter(keyValueBL, sc, writerModel.endpointId.getValue.toHexString)
  }

  override def getSparkReader(id: String, name: String): StaticReader = {
    throw new NotImplementedError("The HBase SparkReader")
  }

  private def startupHBase(wasptimeout: Long)(implicit timeout: Timeout): Unit = {

  }

  override def pluginType: String = "hbase"
}
