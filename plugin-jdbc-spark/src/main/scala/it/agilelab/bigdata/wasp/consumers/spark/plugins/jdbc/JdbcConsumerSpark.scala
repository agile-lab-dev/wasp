package it.agilelab.bigdata.wasp.consumers.spark.plugins.jdbc

import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.readers.{SparkBatchReader, SparkStructuredStreamingReader}
import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkBatchWriter, SparkStructuredStreamingWriter}
import it.agilelab.bigdata.wasp.repository.core.bl.{ConfigBL, SqlSourceBl}
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct.JDBCProduct
import it.agilelab.bigdata.wasp.repository.core.db.WaspDB
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.configuration.ValidationRule
import it.agilelab.bigdata.wasp.models._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

class JdbcConsumerSpark extends WaspConsumersSparkPlugin with Logging {
  var sqlModelBL: SqlSourceBl = _

  override def datastoreProduct: DatastoreProduct = JDBCProduct

  override def initialize(waspDB: WaspDB): Unit = {
    logger.info("Initializing Jdbc consumer spark")
    sqlModelBL = ConfigBL.sqlSourceBl
  }

  override def getValidationRules: Seq[ValidationRule] = Seq()

  override def getSparkStructuredStreamingWriter(ss: SparkSession,
                                                 structuredStreamingETLModel: StructuredStreamingETLModel,
                                                 writerModel: WriterModel): SparkStructuredStreamingWriter = {
    val msg = s"Invalid spark writer type: jdbc spark structured streaming writer"
    logger.error(msg)
    throw new UnsupportedOperationException(msg)
  }
  
  override def getSparkStructuredStreamingReader(ss: SparkSession,
                                                 structuredStreamingETLModel: StructuredStreamingETLModel,
                                                 streamingReaderModel: StreamingReaderModel): SparkStructuredStreamingReader = {
    val msg = s"The datastore product $datastoreProduct is not a valid streaming source! Reader model $streamingReaderModel is not valid."
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