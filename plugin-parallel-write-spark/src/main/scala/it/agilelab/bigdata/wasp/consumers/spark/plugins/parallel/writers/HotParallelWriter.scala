package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.writers

import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.entity.ParallelWriteEntity.CorrelationId
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.entity.{DataStreamRequestBody, ParallelWriteEntity, WriteExecutionPlanResponseBody}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.CatalogCoordinates
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.utils.DataCatalogService
import it.agilelab.bigdata.wasp.core.logging.Logging
import org.apache.spark.sql.functions.{col, struct, to_json}
import org.apache.spark.sql.{DataFrame, Row}

case class HotParallelWriter(
    entityDetails: CatalogCoordinates,
    catalogService: DataCatalogService,
    entityAPI: ParallelWriteEntity
) extends ParallelWriter with Logging {

  /**
    * Writes data according to write execution plan
    *
    * @param writeExecutionPlan execution plan obtained from entity
    * @param df                 data to write
    */
  override def write(
      writeExecutionPlan: WriteExecutionPlanResponseBody,
      df: DataFrame,
      correlationId: CorrelationId,
      batchId: Long
  ): Unit = {
    logger.info(s"Writing to entity ${entityDetails.name}")
    df.select(to_json(struct(df.columns.map(col): _*))).foreachPartition { it: Iterator[Row] =>
      val stream = it.map(r => r.getString(0))
      entityAPI.postDataStream(DataStreamRequestBody(stream), correlationId)
      ()
    }
  }
}
