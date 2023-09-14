package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.writers

import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.{CatalogCoordinates, EntityCatalogBuilder}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.entity.ParallelWriteEntity.CorrelationId
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.entity.{DataCompleteRequestBody, ParallelWriteEntity, WriteExecutionPlanResponseBody}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.utils.CommitStatus.{Failed, Pending}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.utils.CommitStatus
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.utils.{DataCatalogService, DataframeSchemaUtils}
import it.agilelab.bigdata.wasp.core.logging.Logging
import org.apache.spark.sql.DataFrame

import scala.annotation.tailrec
import scala.concurrent.duration.{FiniteDuration, SECONDS}
import scala.util.{Failure, Success}

trait ParallelWriter extends Logging{
  val entityDetails: CatalogCoordinates
  val catalogService: DataCatalogService
  val entityAPI: ParallelWriteEntity
  val numRetries: Int = 5
  val retryPollInterval: FiniteDuration = FiniteDuration(5, SECONDS)

  /**
    * Writes data according to write execution plan
    * @param writeExecutionPlan execution plan obtained from entity
    * @param df data to write
    */
  def write(writeExecutionPlan: WriteExecutionPlanResponseBody, df: DataFrame, correlationId: CorrelationId, batchId: Long): Unit

  def rollback(correlationId: CorrelationId): Unit =
    entityAPI.postDataComplete(DataCompleteRequestBody(false), correlationId)

  def complete(correlationId: CorrelationId): Unit =
    entityAPI.postDataComplete(DataCompleteRequestBody(true), correlationId)

  def commit(correlationId: CorrelationId, batchId: Long): Unit = {
    logger.info(s"Successfully wrote microbatch with id: $batchId")
    logger.info("Polling the entity for commitStatus")
    if (!pollForCommitStatus(numRetries, retryPollInterval, correlationId)) {
      logger.info("Retries exhausted, commit still not finished")
      throw new Throwable("Retries on the entity failed")
    }
  }

  @tailrec
  private def pollForCommitStatus(
                                   retry: Int,
                                   duration: FiniteDuration,
                                   corrId: CorrelationId
                                 ): Boolean = {
    val commitStatus = entityAPI.getDataCommitted(corrId).commitStatus
    if (retry == 0) {
      false
    } else {
      commitStatus match {
        case Pending.status =>
          logger.info(s"Value of CommitStatus is ${commitStatus}")
          Thread.sleep(duration.toMillis)
          pollForCommitStatus(retry - 1, duration, corrId)
        case CommitStatus.Success.status =>
          logger.info(s"Value of CommitStatus is ${commitStatus}")
          true
        case Failed.status =>
          logger.info(s"Value of CommitStatus is ${commitStatus}")
          throw new Throwable("Value of CommitStatus is 'Failed'.\"")
      }
    }
  }

  def enforceSchema(df: DataFrame): DataFrame = {
    val tableSchema = catalogService.getSchema(df.sparkSession, entityDetails)
    DataframeSchemaUtils.convertToSchema(df, tableSchema) match {
      case Failure(ex) =>
        throw new Exception(
          s"Error while trying to write dataframe to table ${EntityCatalogBuilder.getEntityCatalogService().getEntityTableName(entityDetails)}",
          ex
        )
      case Success(d) => d
    }
  }
}
