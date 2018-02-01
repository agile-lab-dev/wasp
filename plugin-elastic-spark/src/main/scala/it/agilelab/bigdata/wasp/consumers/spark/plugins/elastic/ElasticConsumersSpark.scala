package it.agilelab.bigdata.wasp.consumers.spark.plugins.elastic

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, Props}
import akka.pattern.ask
import akka.util.Timeout
import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.readers.SparkReader
import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkLegacyStreamingWriter, SparkWriter}
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.WaspSystem.??
import it.agilelab.bigdata.wasp.core.WaspSystem.waspConfig
import it.agilelab.bigdata.wasp.core.bl.{IndexBL, IndexBLImp}
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.{Datastores, WriterModel}
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
class ElasticConsumersSpark extends WaspConsumersSparkPlugin with Logging {
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

  override def getSparkLegacyStreamingWriter(ssc: StreamingContext, writerModel: WriterModel): SparkLegacyStreamingWriter = {
    logger.info(s"Initialize the elastic spark streaming writer with this writer model id '${writerModel.endpointId.get.getValue.toHexString}'")
    new ElasticSparkLegacyStreamingWriter(indexBL, ssc, writerModel.endpointId.get.getValue.toHexString, elasticAdminActor_)
  }

  override def getSparkStructuredStreamingWriter(ss: SparkSession, writerModel: WriterModel) = {
    logger.info(s"Initialize the elastic spark structured streaming writer with this writer model id '${writerModel.endpointId.get.getValue.toHexString}'")
    new ElasticSparkStructuredStreamingWriter(indexBL, ss, writerModel.endpointId.get.getValue.toHexString, elasticAdminActor_)
  }

  override def getSparkWriter(sc: SparkContext, writerModel: WriterModel): SparkWriter = {
    logger.info(s"Initialize the elastic spark batch writer with this writer model id '${writerModel.endpointId.get.getValue.toHexString}'")
    new ElasticSparkWriter(indexBL, sc, writerModel.endpointId.get.getValue.toHexString, elasticAdminActor_)
  }

  override def getSparkReader(id: String, name: String): SparkReader = {
    val indexOpt = indexBL.getById(id)
    if (indexOpt.isDefined) {
      val index = indexOpt.get
      val indexName = index.eventuallyTimedName

      logger.info(
        s"Check or create the index model: '${index.toString} with this index name: $indexName")

      if (index.schema.isEmpty) {
        throw new Exception(
          s"There no define schema in the index configuration: $index")
      }
      if (index.name.toLowerCase != index.name) {
        throw new Exception(s"The index name must be all lowercase: $index")
      }

      if (??[Boolean](
        elasticAdminActor_,
        CheckOrCreateIndex(
          indexName,
          index.name,
          index.dataType,
          index.getJsonSchema))) {

        new ElasticSparkReader(indexOpt.get)

      } else {
        val msg = s"Error creating elastic index: $index with this index name $indexName"
        logger.error(msg)
        throw new Exception(msg)
      }
    } else {
      val msg = s"Elastic spark reader indexOption not found: id: '$id, name: $name'"
      logger.error(msg)
      throw new Exception(msg)
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

  override def pluginType: String = Datastores.elasticProduct
}
