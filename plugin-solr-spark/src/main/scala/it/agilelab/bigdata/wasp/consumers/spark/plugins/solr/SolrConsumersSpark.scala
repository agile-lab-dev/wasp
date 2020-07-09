package it.agilelab.bigdata.wasp.consumers.spark.plugins.solr

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, Props}
import akka.pattern.ask
import akka.util.Timeout
import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.readers.{SparkBatchReader, SparkLegacyStreamingReader, SparkStructuredStreamingReader}
import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkBatchWriter, SparkLegacyStreamingWriter}
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.WaspSystem.??
import it.agilelab.bigdata.wasp.core.WaspSystem.waspConfig
import it.agilelab.bigdata.wasp.repository.core.bl.{ConfigBL, IndexBL}
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct.SolrProduct
import it.agilelab.bigdata.wasp.repository.core.db.WaspDB
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.configuration.ValidationRule
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import it.agilelab.bigdata.wasp.models.{LegacyStreamingETLModel, ReaderModel, StreamingReaderModel, StructuredStreamingETLModel, WriterModel}
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

  override def datastoreProduct: DatastoreProduct = SolrProduct

  override def initialize(waspDB: WaspDB): Unit = {
    logger.info("Initialize the index BL")
    indexBL = ConfigBL.indexBL
    logger.info(s"Initialize the solr admin actor with this name ${SolrAdminActor.name}")
    solrAdminActor_ = WaspSystem.actorSystem.actorOf(Props(new SolrAdminActor), SolrAdminActor.name)
    // services timeout, used below
    val servicesTimeoutMillis = waspConfig.servicesTimeoutMillis
    // implicit timeout used below
    implicit val timeout: Timeout = new Timeout(servicesTimeoutMillis - 1000, TimeUnit.MILLISECONDS)
    startupSolr(servicesTimeoutMillis)
  }

  override def getValidationRules: Seq[ValidationRule] = Seq()

  override def getSparkLegacyStreamingWriter(ssc: StreamingContext,
                                             legacyStreamingETLModel: LegacyStreamingETLModel,
                                             writerModel: WriterModel): SparkLegacyStreamingWriter = {
    logger.info(s"Initialize the solr spark streaming writer with this writer model name '${writerModel.datastoreModelName}'")
    new SolrSparkLegacyStreamingWriter(indexBL, ssc, writerModel.datastoreModelName, solrAdminActor_)
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
                                                 writerModel: WriterModel): SolrSparkStructuredStreamingWriter = {
    logger.info(s"Initialize the solr spark structured streaming writer with this writer model endpointName '${writerModel.datastoreModelName}'")
    new SolrSparkStructuredStreamingWriter(indexBL, ss, writerModel.datastoreModelName, solrAdminActor_)
  }
  
  override def getSparkStructuredStreamingReader(ss: SparkSession,
                                                 structuredStreamingETLModel: StructuredStreamingETLModel,
                                                 streamingReaderModel: StreamingReaderModel): SparkStructuredStreamingReader = {
    val msg = s"The datastore product $datastoreProduct is not a valid streaming source! Reader model $streamingReaderModel is not valid."
    logger.error(msg)
    throw new UnsupportedOperationException(msg)
  }

  override def getSparkBatchWriter(sc: SparkContext, writerModel: WriterModel): SparkBatchWriter = {
    logger.info(s"Initialize the solr spark batch writer with this writer model id '${writerModel.datastoreModelName}'")
    new SolrSparkBatchWriter(indexBL, sc, writerModel.datastoreModelName, solrAdminActor_)
  }

  override def getSparkBatchReader(sc: SparkContext, readerModel: ReaderModel): SparkBatchReader = {
    val indexOpt = indexBL.getByName(readerModel.name)
    if (indexOpt.isDefined) {
      val index = indexOpt.get
      val indexName = index.eventuallyTimedName

      logger.info(s"Check or create the index model: '${index.toString} with this index name: $indexName")

      if (??[Boolean](
          solrAdminActor_,
          CheckOrCreateCollection(
            indexName,
            index.getJsonSchema,
            index.numShards.getOrElse(1),
            index.replicationFactor.getOrElse(1)))) {

        new SolrSparkBatchReader(index)

      } else {
        val msg = s"Error creating solr index: $index with this index name $indexName"
        logger.error(msg)
        throw new Exception(msg)
      }
    } else {
      val msg = s"Index model not found: $readerModel"
      logger.error(msg)
      throw new Exception(msg)
    }
  }

  private def startupSolr(servicesTimeoutMillis: Long)(implicit timeout: Timeout): Unit = {
    logger.info(s"Trying to connect with Solr...")

    //TODO if solrConfig are not initialized skip the initialization
    val solrResult = solrAdminActor_ ? Initialization(ConfigManager.getSolrConfig)

    val solrConnectionResult = Await.ready(solrResult, Duration(servicesTimeoutMillis, TimeUnit.MILLISECONDS))

    solrConnectionResult.value match {
      case Some(Failure(t)) =>
        logger.error(t.getMessage)
        throw new Exception(t)

      case Some(Success(_)) =>
        logger.info("The system is connected with Solr")

      case None => throw new UnknownError("Unknown error during Solr connection initialization")
    }
  }
}