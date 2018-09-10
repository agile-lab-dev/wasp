package it.agilelab.bigdata.wasp.consumers.spark.plugins.jdbc

import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.readers.SparkBatchReader
import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkLegacyStreamingWriter, SparkStructuredStreamingWriter, SparkBatchWriter}
import it.agilelab.bigdata.wasp.core.bl.{SqlSourceBl, SqlSourceBlImpl}
import it.agilelab.bigdata.wasp.core.datastores.DatastoreProduct
import it.agilelab.bigdata.wasp.core.datastores.DatastoreProduct.JDBCProduct
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.configuration.ValidationRule
import it.agilelab.bigdata.wasp.core.models.{LegacyStreamingETLModel, ReaderModel, StructuredStreamingETLModel, WriterModel}
import it.agilelab.bigdata.wasp.core.utils.WaspDB
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

class JdbcConsumerSpark extends WaspConsumersSparkPlugin with Logging {
  var sqlModelBL: SqlSourceBl = _

  override def datastoreProduct: DatastoreProduct = JDBCProduct

  override def initialize(waspDB: WaspDB): Unit = {
    logger.info("Initializing Jdbc consumer spark")
    sqlModelBL = new SqlSourceBlImpl(waspDB)
  }

  override def getValidationRules: Seq[ValidationRule] = Seq()

  override def getSparkLegacyStreamingWriter(ssc: StreamingContext,
                                             legacyStreamingETLModel: LegacyStreamingETLModel,
                                             writerModel: WriterModel): SparkLegacyStreamingWriter = {
    val msg = s"Invalid spark writer type: jdbc spark streaming writer"
    logger.error(msg)
    throw new UnsupportedOperationException(msg)
  }

  override def getSparkStructuredStreamingWriter(ss: SparkSession,
                                                 structuredStreamingETLModel: StructuredStreamingETLModel,
                                                 writerModel: WriterModel): SparkStructuredStreamingWriter = {
    val msg = s"Invalid spark writer type: jdbc spark structured streaming writer"
    logger.error(msg)
    throw new UnsupportedOperationException(msg)
  }

  override def getSparkBatchWriter(sc: SparkContext, writerModel: WriterModel): SparkBatchWriter = {
    val msg = s"Invalid spark writer type: jdbc spark batch writer"
    logger.error(msg)
    throw new UnsupportedOperationException(msg)
  }

  override def getSparkBatchReader(sc: SparkContext, readerModel: ReaderModel): SparkBatchReader = {
    logger.info(s"Initialize JdbcReader with model $readerModel")
    val sqlOpt = sqlModelBL.getByName(readerModel.name)
    val sqlSource =
      if (sqlOpt.isDefined) {
        sqlOpt.get
      } else {
        val msg = s"SQL source model not found: $readerModel"
        logger.error(msg)
        throw new Exception(msg)
      }

    new JDBCSparkBatchReader(sqlSource)
  }
}