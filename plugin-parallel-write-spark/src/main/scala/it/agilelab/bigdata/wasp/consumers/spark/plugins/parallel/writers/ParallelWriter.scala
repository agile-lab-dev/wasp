package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.writers

import it.agilelab.bigdata.microservicecatalog.entity.WriteExecutionPlanResponseBody
import org.apache.spark.sql.DataFrame

trait ParallelWriter {
  /**
   * Writes data according to write execution plan
   * @param writeExecutionPlan execution plan obtained from entity
   * @param df data to write
   */
  def write(writeExecutionPlan: WriteExecutionPlanResponseBody, df: DataFrame): Unit
}
