package it.agilelab.bigdata.wasp.consumers.spark.plugins.solr

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, Props}
import akka.pattern.ask
import akka.util.Timeout
import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.readers.SparkReader
import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkLegacyStreamingWriter, SparkWriter}
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
class SolrConsumersSpark extends WaspConsumersSparkPlugin with Logging {
  var indexBL: IndexBL = _
  var solrAdminActor_ : ActorRef = _

  override def initialize(waspDB: WaspDB): Unit = {
    logger.info("Initialize the index BL")
    indexBL = new IndexBLImp(waspDB)
    logger.info(s"Initialize the solr admin actor with this name ${SolrAdminActor.name}")
    solrAdminActor_ = WaspSystem.actorSystem.actorOf(Props(new SolrAdminActor), SolrAdminActor.name)
    // services timeout, used below
    val servicesTimeoutMillis = waspConfig.servicesTimeoutMillis
    // implicit timeout used below
    implicit val timeout: Timeout = new Timeout(servicesTimeoutMillis, TimeUnit.MILLISECONDS)
    startupSolr(servicesTimeoutMillis)
  }

  override def getSparkLegacyStreamingWriter(ssc: StreamingContext, writerModel: WriterModel): SparkLegacyStreamingWriter = {
    logger.info(s"Initialize the solr spark streaming writer with this writer model id '${writerModel.endpointId.getValue.toHexString}'")
    new SolrSparkLegacyStreamingWriter(indexBL, ssc, writerModel.endpointId.getValue.toHexString, solrAdminActor_)
  }

  override def getSparkStructuredStreamingWriter(ss: SparkSession, writerModel: WriterModel) = ???

  override def getSparkWriter(sc: SparkContext, writerModel: WriterModel): SparkWriter = {
    logger.info(s"Initialize the solr spark batch writer with this writer model id '${writerModel.endpointId.getValue.toHexString}'")
    new SolrSparkWriter(indexBL, sc, writerModel.endpointId.getValue.toHexString, solrAdminActor_)
  }

  override def getSparkReader(id: String, name: String): SparkReader = {
    val indexOpt = indexBL.getById(id)
    if (indexOpt.isDefined) {
      new SolrReader(indexOpt.get)
    } else {
      throw new Exception("Solr Option not found")
    }
  }

  private def startupSolr(wasptimeout: Long)(implicit timeout: Timeout): Unit = {
    logger.info(s"Trying to connect with Sol...")

    //TODO if solrConfig are not initialized skip the initialization
    val solrResult = solrAdminActor_ ? Initialization(ConfigManager.getSolrConfig)
    val solrConnectionResult = Await.ready(solrResult, Duration(wasptimeout, TimeUnit.SECONDS))

    solrConnectionResult.value match {
      case Some(Failure(t)) =>
        logger.error(t.getMessage)
        throw new Exception(t)

      case Some(Success(_)) =>
        logger.info("The system is connected with Solr")

      case None => throw new UnknownError("Unknown error during Solr connection initialization")
    }
  }

  override def pluginType: String = "solr"
}
