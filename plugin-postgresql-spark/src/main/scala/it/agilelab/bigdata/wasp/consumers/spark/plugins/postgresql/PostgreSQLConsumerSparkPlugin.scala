package it.agilelab.bigdata.wasp.consumers.spark.plugins.postgresql

import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.readers.{SparkBatchReader, SparkStructuredStreamingReader}
import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkBatchWriter, SparkStructuredStreamingWriter}
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct.PostgreSQLProduct
import it.agilelab.bigdata.wasp.repository.core.db.WaspDB
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.configuration.ValidationRule
import it.agilelab.bigdata.wasp.models._
import it.agilelab.bigdata.wasp.repository.core.bl.{ConfigBL, SQLSinkBL}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

/**
	* A WASP Consumers Spark plugin to write to PostgreSQL via batch and streaming, with support for `INSERT ON CONFLICT`
	* operations, connection pooling, batching and transactions.
	*
	* @author Nicolò Bidotti
	*/
class PostgreSQLConsumerSparkPlugin extends WaspConsumersSparkPlugin with Logging {
  import PostgreSQLConsumerSparkPlugin._

  var sqlSinkBL: SQLSinkBL = _

  override def datastoreProduct: DatastoreProduct = PostgreSQLProduct

  override def initialize(waspDB: WaspDB): Unit = {
    sqlSinkBL = ConfigBL.sqlSinkBL
    logger.info("Plugin initialized")
  }

  override def getValidationRules: Seq[ValidationRule] = Seq() // TODO implement validation rules

  override def getSparkStructuredStreamingWriter(
      ss: SparkSession,
      structuredStreamingETLModel: StructuredStreamingETLModel,
      writerModel: WriterModel
  ): SparkStructuredStreamingWriter = {
    validateWriterModel(writerModel)
    val sqlSinkModelName  = writerModel.datastoreModelName
    val maybeSqlSinkModel = sqlSinkBL.getByName(sqlSinkModelName)
    maybeSqlSinkModel match {
      case Some(sqlSinkModel: SQLSinkModel) => new PostgreSQLSparkStructuredStreamingWriter(sqlSinkModel)
      case None                             => throw new Exception(s"Unable to find SQLSinkModel with name $sqlSinkModelName")
    }
  }

  override def getSparkStructuredStreamingReader(
      ss: SparkSession,
      structuredStreamingETLModel: StructuredStreamingETLModel,
      streamingReaderModel: StreamingReaderModel
  ): SparkStructuredStreamingReader = {
    val msg =
      s"The datastore product $datastoreProduct is not a valid streaming source! Reader model $streamingReaderModel is not valid."
    logger.error(msg)
    throw new UnsupportedOperationException(msg)
  }

  override def getSparkBatchWriter(sc: SparkContext, writerModel: WriterModel): SparkBatchWriter = {
    validateWriterModel(writerModel)
    val sqlSinkModelName  = writerModel.datastoreModelName
    val maybeSqlSinkModel = sqlSinkBL.getByName(sqlSinkModelName)
    maybeSqlSinkModel match {
      case Some(sqlSinkModel: SQLSinkModel) => new PostgreSQLSparkBatchWriter(sqlSinkModel)
      case None                             => throw new Exception(s"Unable to find SQLSinkModel with name $sqlSinkModelName")
    }
  }

  override def getSparkBatchReader(sc: SparkContext, readerModel: ReaderModel): SparkBatchReader = {
    val msg =
      s"The datastore product $datastoreProduct is not a valid batch source! Reader model $readerModel is not valid."
    logger.error(msg)
    throw new UnsupportedOperationException(msg)
  }
}

object PostgreSQLConsumerSparkPlugin {
  private[postgresql] def validateWriterModel(writerModel: WriterModel): Unit = {
    require(
      writerModel.datastoreProduct == PostgreSQLProduct,
      s"Unsupported datastore product ${writerModel.datastoreProduct}"
    )
  }

  private[postgresql] def validateSQLSinkModelAgainstSchema(schema: StructType, sqlSinkModel: SQLSinkModel): Unit = {
    val schemaFields  = schema.map(_.name).toSet
    val pkFields      = sqlSinkModel.primaryKeys
    val updatedFields = sqlSinkModel.updateClauses.map(_.keys.toList).getOrElse(List.empty[String])
    require(pkFields.forall(schemaFields), "All columns in the primary key must be present in the schema")
    require(
      updatedFields.forall(schemaFields),
      "All columns updated by the update clauses must be present in the schema"
    )
  }

}
