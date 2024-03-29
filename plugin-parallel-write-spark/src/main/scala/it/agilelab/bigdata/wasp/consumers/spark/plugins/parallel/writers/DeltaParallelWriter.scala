package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.writers

import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.CatalogCoordinates
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.entity.ParallelWriteEntity
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.model.ParallelWrite
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.utils.DataCatalogService
import org.apache.spark.sql.DataFrame

import java.net.URI

case class DeltaParallelWriter(
  parallelWriteDetails: ParallelWrite,
  entityAPI: ParallelWriteEntity,
  entityDetails: CatalogCoordinates,
  override val catalogService: DataCatalogService
) extends DeltaParallelWriterTrait {

  override def performDeltaWrite(df: DataFrame, path: URI, partitioningColumns: Seq[String], batchId: Long): Unit =
    enforceSchema(df).write
      .mode(parallelWriteDetails.saveMode)
      .format("delta")
      .partitionBy(partitioningColumns: _*)
      .save(path.toString)
}
