package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.writers

import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.CatalogCoordinates
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.model.ParallelWrite
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.utils.DataCatalogService
import org.apache.spark.sql.DataFrame

import java.net.URI

case class ParquetParallelWriter(
  parallelWriteDetails: ParallelWrite,
  entityDetails: CatalogCoordinates,
  catalogService: DataCatalogService
) extends ColdParallelWriter {

  override protected def performColdWrite(df: DataFrame, s3path: URI, partitioningColumns: Seq[String]): Unit =
    enforceSchema(df).write
      .mode(parallelWriteDetails.saveMode)
      .format("parquet")
      .partitionBy(partitioningColumns: _*)
      .save(s3path.toString)
}
