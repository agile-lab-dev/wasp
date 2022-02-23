package it.agilelab.bigdata.wasp.consumers.spark.plugins.elastic

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, Props}
import akka.pattern.ask
import akka.util.Timeout
import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.readers.{SparkBatchReader, SparkStructuredStreamingReader}
import it.agilelab.bigdata.wasp.consumers.spark.writers.SparkBatchWriter
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.WaspSystem.??
import it.agilelab.bigdata.wasp.core.WaspSystem.waspConfig
import it.agilelab.bigdata.wasp.repository.core.bl.{ConfigBL, IndexBL}
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct.ElasticProduct
import it.agilelab.bigdata.wasp.repository.core.db.WaspDB
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.configuration.ValidationRule
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import it.agilelab.bigdata.wasp.models.{ReaderModel, StreamingReaderModel, StructuredStreamingETLModel, WriterModel}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}


/**
  * Created by Agile Lab s.r.l. on 05/09/2017.
  */
class ElasticConsumersSpark extends WaspConsumersSparkPlugin with Logging {
  var indexBL: IndexBL = _
  var elasticAdminActor_ : ActorRef = _

  override def datastoreProduct: DatastoreProduct = ElasticProduct

  override def initialize(waspDB: WaspDB): Unit = {
    logger.info("Initialize the index BL")
    indexBL = ConfigBL.indexBL
    logger.info(s"Initialize the elastic admin actor with this name ${ElasticAdminActor.name}")
    elasticAdminActor_ = WaspSystem.actorSystem
      .actorOf(Props(new ElasticAdminActor), ElasticAdminActor.name)
    // services timeout, used below
    val servicesTimeoutMillis = waspConfig.servicesTimeoutMillis
    // implicit timeout used below
    implicit val timeout: Timeout = new Timeout(servicesTimeoutMillis - 1000, TimeUnit.MILLISECONDS)
    startupElastic(servicesTimeoutMillis)
  }

  override def getValidationRules: Seq[ValidationRule] = Seq()

  override def getSparkStructuredStreamingWriter(ss: SparkSession,
                                                 structuredStreamingETLModel: StructuredStreamingETLModel,
                                                 writerModel: WriterModel): ElasticsearchSparkStructuredStreamingWriter = {
    logger.info(s"Initialize the elastic spark structured streaming writer with this writer model name '${writerModel.name}'")
    new ElasticsearchSparkStructuredStreamingWriter(indexBL, ss, writerModel.datastoreModelName, elasticAdminActor_)
  }
  
  override def getSparkStructuredStreamingReader(ss: SparkSession,
                                                 structuredStreamingETLModel: StructuredStreamingETLModel,
                                                 streamingReaderModel: StreamingReaderModel): SparkStructuredStreamingReader = {
    val msg = s"The datastore product $datastoreProduct is not a valid streaming source! Reader model $streamingReaderModel is not valid."
    logger.error(msg)
    throw new UnsupportedOperationException(msg)
  }

  override def getSparkBatchWriter(sc: SparkContext, writerModel: WriterModel): SparkBatchWriter = {
    logger.info(s"Initialize the elastic spark batch writer with this writer model name '${writerModel.name}'")
    new ElasticsearchSparkBatchWriter(indexBL, sc, writerModel.name, elasticAdminActor_)
  }

  override def getSparkBatchReader(sc: SparkContext, readerModel: ReaderModel): SparkBatchReader = {
    val indexOpt = indexBL.getByName(readerModel.name)
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

        new ElasticsearchSparkBatchReader(index)

      } else {
        val msg = s"Error creating elastic index: $index with this index name $indexName"
        logger.error(msg)
        throw new Exception(msg)
      }
    } else {
      val msg = s"Index model not found: $readerModel"
      logger.error(msg)
      throw new Exception(msg)
    }
  }

  private def startupElastic(servicesTimeoutMillis: Long)(implicit timeout: Timeout): Unit = {
    logger.info(s"Trying to connect with Elastic...")

    //TODO if elasticConfig are not initialized skip the initialization
    val elasticResult = elasticAdminActor_ ? Initialization(ConfigManager.getElasticConfig)

    val elasticConnectionResult = Await.ready(elasticResult, Duration(servicesTimeoutMillis, TimeUnit.MILLISECONDS))

    elasticConnectionResult.value match {
      case Some(Failure(t)) =>
        logger.error(t.getMessage)
        throw new Exception(t)

      case Some(Success(_)) =>
        logger.info("The system is connected with Elastic")

      case None => throw new UnknownError("Unknown error during Elastic connection initialization")
    }
  }
}
