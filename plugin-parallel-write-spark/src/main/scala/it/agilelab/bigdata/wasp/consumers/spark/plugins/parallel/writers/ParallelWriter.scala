package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.writers

import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.CatalogCoordinates
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.entity.WriteExecutionPlanResponseBody
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.utils.DataCatalogService
import org.apache.spark.sql.DataFrame

trait ParallelWriter {
  val entityDetails: CatalogCoordinates
  val catalogService: DataCatalogService

  /**
   * Writes data according to write execution plan
   * @param writeExecutionPlan execution plan obtained from entity
   * @param df data to write
   */
  def write(writeExecutionPlan: WriteExecutionPlanResponseBody, df: DataFrame): Unit
}
