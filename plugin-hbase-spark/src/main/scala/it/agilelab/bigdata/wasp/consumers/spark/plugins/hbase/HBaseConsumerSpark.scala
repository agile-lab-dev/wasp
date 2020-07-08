package it.agilelab.bigdata.wasp.consumers.spark.plugins.hbase

import java.util.concurrent.TimeUnit

import akka.actor.ActorRef
import akka.util.Timeout
import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.readers.{SparkBatchReader, SparkLegacyStreamingReader, SparkStructuredStreamingReader}
import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkBatchWriter, SparkLegacyStreamingWriter, SparkStructuredStreamingWriter}
import it.agilelab.bigdata.wasp.core.WaspSystem.waspConfig
import it.agilelab.bigdata.wasp.core.bl.{ConfigBL, KeyValueBL}
import it.agilelab.bigdata.wasp.core.datastores.DatastoreProduct
import it.agilelab.bigdata.wasp.core.datastores.DatastoreProduct.HBaseProduct
import it.agilelab.bigdata.wasp.core.db.WaspDB
import it.agilelab.bigdata.wasp.core.exceptions.ModelNotFound
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.configuration.ValidationRule
import it.agilelab.bigdata.wasp.core.models._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext


/**
  * Created by Agile Lab s.r.l. on 05/09/2017.
  */
class HBaseConsumerSpark extends WaspConsumersSparkPlugin with Logging {
  var keyValueBL: KeyValueBL = _
  var hbaseAdminActor_ : ActorRef = _

  override def datastoreProduct: DatastoreProduct = HBaseProduct

  override def initialize(waspDB: WaspDB): Unit = {
    logger.info("Initialize the keyValue BL")
    keyValueBL = ConfigBL.keyValueBL
    logger.info(s"Initialize the hbase admin actor with this name ${HBaseAdminActor.name}")
    //hbaseAdminActor_ = WaspSystem.actorSystem
    //  .actorOf(Props(new HBaseAdminActor), HBaseAdminActor.name)
    // services timeout, used below
    val servicesTimeoutMillis = waspConfig.servicesTimeoutMillis
    // implicit timeout used below
    implicit val timeout: Timeout = new Timeout(servicesTimeoutMillis, TimeUnit.MILLISECONDS)
    startupHBase(servicesTimeoutMillis)
  }

  override def getValidationRules: Seq[ValidationRule] = Seq.empty

  private def startupHBase(wasptimeout: Long)(implicit timeout: Timeout): Unit = {

  }

  override def getSparkLegacyStreamingWriter(ssc: StreamingContext,
                                             legacyStreamingETLModel: LegacyStreamingETLModel,
                                             writerModel: WriterModel): SparkLegacyStreamingWriter = {
    logger.info(s"Initialize the hbase spark streaming writer with this writer model name '${writerModel.name}'")
    HBaseBatchWriter.createSparkStreamingWriter(keyValueBL, ssc, getKeyValueModel(writerModel))
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
                                                 writerModel: WriterModel): SparkStructuredStreamingWriter = {
    HBaseBatchWriter.createSparkStructuredStreamingWriter(keyValueBL, ss, getKeyValueModel(writerModel))
  }
  
  override def getSparkStructuredStreamingReader(ss: SparkSession,
                                                 structuredStreamingETLModel: StructuredStreamingETLModel,
                                                 streamingReaderModel: StreamingReaderModel): SparkStructuredStreamingReader = {
    val msg = s"The datastore product $datastoreProduct is not a valid streaming source! Reader model $streamingReaderModel is not valid."
    logger.error(msg)
    throw new UnsupportedOperationException(msg)
  }

  override def getSparkBatchWriter(sc: SparkContext, writerModel: WriterModel): SparkBatchWriter = {
    logger.info(s"Initialize the hbase spark batch writer with this writer model name '${writerModel.name}'")
    HBaseBatchWriter.createSparkWriter(keyValueBL, sc, getKeyValueModel(writerModel))
  }

  override def getSparkBatchReader(sc: SparkContext, readerModel: ReaderModel): SparkBatchReader = {
    logger.info(s"Creating HBase Spark batch reader for key value model $readerModel")
    val hbaseOpt = keyValueBL.getByName(readerModel.name)
    val keyValue =
      if (hbaseOpt.isDefined) {
        hbaseOpt.get
      } else {
        val msg = s"Key value model not found: $readerModel"
        logger.error(msg)
        throw new Exception(msg)
      }

    new HBaseSparkBatchReader(keyValue)
  }

  @throws(classOf[ModelNotFound])
  private def getKeyValueModel(writerModel: WriterModel): KeyValueModel = {

    val endpointName = writerModel.datastoreModelName
    val hbaseModelOpt = keyValueBL.getByName(endpointName)
    if (hbaseModelOpt.isDefined) {
      hbaseModelOpt.get
    } else {
      throw new ModelNotFound(s"The KeyValueModel with this name $endpointName not found")
    }
  }
}
