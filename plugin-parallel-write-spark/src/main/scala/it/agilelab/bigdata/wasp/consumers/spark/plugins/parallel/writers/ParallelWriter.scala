package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.writers

import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.{CatalogCoordinates, EntityCatalogBuilder}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.entity.WriteExecutionPlanResponseBody
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.utils.{DataCatalogService, DataframeSchemaUtils}
import org.apache.spark.sql.DataFrame

import scala.util.{Failure, Success}

trait ParallelWriter {
  val entityDetails: CatalogCoordinates
  val catalogService: DataCatalogService

  /**
   * Writes data according to write execution plan
   * @param writeExecutionPlan execution plan obtained from entity
   * @param df data to write
   */
  def write(writeExecutionPlan: WriteExecutionPlanResponseBody, df: DataFrame): Unit

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
