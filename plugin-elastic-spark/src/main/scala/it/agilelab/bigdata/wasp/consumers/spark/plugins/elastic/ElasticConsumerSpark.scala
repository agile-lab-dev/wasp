package it.agilelab.bigdata.wasp.consumers.spark.plugins.elastic

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, Props}
import akka.pattern.ask
import akka.util.Timeout
import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumerSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.readers.StaticReader
import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkStreamingWriter, SparkWriter}
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.WaspSystem.waspConfig
import it.agilelab.bigdata.wasp.core.bl.{IndexBL, IndexBLImp}
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.WriterModel
import it.agilelab.bigdata.wasp.core.utils.{ConfigManager, WaspDB}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}


/**
  * Created by Agile Lab s.r.l. on 05/09/2017.
  */
class ElasticConsumerSpark extends WaspConsumerSparkPlugin with Logging {
  var indexBL: IndexBL = _
  var elasticAdminActor_ : ActorRef = _

  override def initialize(waspDB: WaspDB): Unit = {
    logger.info("Initialize the index BL")
    indexBL = new IndexBLImp(waspDB)
    logger.info(s"Initialize the elastic admin actor with this name ${ElasticAdminActor.name}")
    elasticAdminActor_ = WaspSystem.actorSystem
      .actorOf(Props(new ElasticAdminActor), ElasticAdminActor.name)
    // services timeout, used below
    val servicesTimeoutMillis = waspConfig.servicesTimeoutMillis
    // implicit timeout used below
    implicit val timeout: Timeout = new Timeout(servicesTimeoutMillis, TimeUnit.MILLISECONDS)
    startupElastic(servicesTimeoutMillis)
  }

  override def getSparkStreamingWriter(ssc: StreamingContext, writerModel: WriterModel): SparkStreamingWriter = {
    logger.info(s"Initialize the elastic spark streaming writer with this writer model id '${writerModel.endpointId.getValue.toHexString}'")
    new ElasticSparkStreamingWriter(indexBL, ssc, writerModel.endpointId.getValue.toHexString, elasticAdminActor_)
  }

  override def getSparkStructuredStreamingWriter(ss: SparkSession, writerModel: WriterModel) = {
    logger.info(s"Initialize the elastic spark structured streaming writer with this writer model id '${writerModel.endpointId.getValue.toHexString}'")
    new ElasticSparkStructuredStreamingWriter(indexBL, ss, writerModel.endpointId.getValue.toHexString, elasticAdminActor_)
  }

  override def getSparkWriter(sc: SparkContext, writerModel: WriterModel): SparkWriter = {
    logger.info(s"Initialize the elastic spark batch writer with this writer model id '${writerModel.endpointId.getValue.toHexString}'")
    new ElasticSparkWriter(indexBL, sc, writerModel.endpointId.getValue.toHexString, elasticAdminActor_)
  }

  override def getSparkReader(id: String, name: String): StaticReader = {
    val indexOpt = indexBL.getById(id)
    if (indexOpt.isDefined) {
      new ElasticIndexReader(indexOpt.get)
    } else {
      logger.error(s"Elastic spark reader not found: id: '$id, name: $name'")
      throw new Exception(s"Elastic spark reader not found: id: '$id, name: $name'")
    }
  }

  private def startupElastic(wasptimeout: Long)(implicit timeout: Timeout): Unit = {
    logger.info(s"Trying to connect with Elastic...")

    //TODO if elasticConfig are not initialized skip the initialization
    val elasticResult = elasticAdminActor_ ? Initialization(ConfigManager.getElasticConfig)

    //TODO remove infinite waiting and enable index swapping
    val elasticConnectionResult = Await.ready(elasticResult, Duration(wasptimeout, TimeUnit.SECONDS))

    elasticConnectionResult.value match {
      case Some(Failure(t)) =>
        logger.error(t.getMessage)
        throw new Exception(t)

      case Some(Success(_)) =>
        logger.info("The system is connected with Elastic")

      case None => throw new UnknownError("Unknown error during Elastic connection initialization")
    }
  }

  override def pluginType: String = "elastic"
}
