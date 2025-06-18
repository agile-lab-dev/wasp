package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.writers

import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.CatalogCoordinates
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.entity.ParallelWriteEntity
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.model.ContinuousUpdate
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.utils.DataCatalogService
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, expr, first}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.net.URI

class SchemaException(message: String) extends Exception(message)

/** Writer for continuous update.
  * @param writerDetails
  *   Informations about unique keys, ordering expression and fields to drop
  * @param entityDetails
  */
case class ContinuousUpdateWriter(
    writerDetails: ContinuousUpdate,
    entityAPI: ParallelWriteEntity,
    entityDetails: CatalogCoordinates,
    catalogService: DataCatalogService
) extends DeltaParallelWriterTrait {

  override def performDeltaWrite(df: DataFrame, s3path: URI, partitioningColumns: Seq[String], batchId: Long): Unit = {
    // schema evolution not supported yet, property not necessary at the moment
    // ss.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
    val spark: SparkSession = df.sparkSession
    val orderedDF           = applyOrderingLogic(df, writerDetails.keys, writerDetails.orderingExpression)
    val enforcedDf          = enforceSchema(orderedDF)
    val deduplicatedDf      = enforcedDf.distinct()
    val condition           = writerDetails.keys.map(x => s"table.$x = table2.$x").mkString(" AND ")
    val deltaTable          = getDeltaTable(s3path, spark, partitioningColumns)

    (writerDetails.enableDeltaOptimize, writerDetails.compactFrequency, writerDetails.compactNumFile) match {
      case (Some(true), Some(compactFrequency), _) if batchId % compactFrequency == 0 && batchId > 0 =>
        logger.info(s"Using Delta Optimize for table at ${s3path} for batch $batchId")
        deltaTable.optimize().executeCompaction()

      case (Some(false) | None, Some(compactFrequency), Some(compactNumFile))
          if batchId % compactFrequency == 0 && batchId > 0 =>
        logger.info(s"Compacting table at ${s3path} with partitions $compactNumFile files")
        deltaTable.toDF
          .repartition(compactNumFile)
          .write
          .option("dataChange", "false")
          .format("delta")
          .mode("overwrite")
          .partitionBy(partitioningColumns: _*)
          .save(s3path.toString)

      case _ => logger.info("No compaction required for this batch.")
    }

    // Vacuuming logic
    (writerDetails.retentionHours, writerDetails.vacuumFrequency) match {
      case (None, None) =>
      case (Some(retentionHours), Some(vacuumFrequency)) =>
        if (batchId % vacuumFrequency == 0 && batchId > 0) {
          logger.info(s"Vacuuming table ${s3path} with retention ${retentionHours} hours")
          deltaTable.vacuum(retentionHours.toDouble)
        }
      case other =>
        throw new IllegalArgumentException(
          s"Both retentionHours and vacuumFrequency must be null or have a value, but ${other} was provided"
        )
    }

    deltaTable
      .as("table")
      .merge(deduplicatedDf.as("table2"), condition)
      .whenMatched()
      .updateAll()
      .whenNotMatched()
      .insertAll()
      .execute()
  }

  private def applyOrderingLogic(df: DataFrame, keys: List[String], orderingExpression: String): DataFrame = {

    val orderingColName: String = "_orderWaspDeltaLake_"
    val dfWithOrderingColumn    = df.withColumn(orderingColName, expr(orderingExpression))
    val windowSpec              = Window.partitionBy(keys.map(col): _*).orderBy(col(orderingColName).desc)

    dfWithOrderingColumn
      .withColumn(
        "max_" + orderingColName,
        first(col(orderingColName))
          .over(windowSpec)
          .as("max_" + orderingColName)
      )
      .filter(orderingColName + " = max_" + orderingColName)
      .drop("max_" + orderingColName)
      .drop(orderingColName)
  }

}
