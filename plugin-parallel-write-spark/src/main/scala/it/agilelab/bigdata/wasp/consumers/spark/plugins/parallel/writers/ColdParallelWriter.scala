package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.writers
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.entity.ParallelWriteEntity.CorrelationId
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.entity.WriteExecutionPlanResponseBody
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.utils.HadoopS3Utils
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.net.URI

trait ColdParallelWriter extends ParallelWriter {

  val credentialsConfigurator: CredentialsConfigurator =
    CredentialsConfigurator.coldAreaCredentialsPersisterConfigurator

  final override def write(
      writeExecutionPlan: WriteExecutionPlanResponseBody,
      df: DataFrame,
      correlationId: CorrelationId,
      batchId: Long
  ): Unit = {
    val s3path: URI = HadoopS3Utils.useS3aScheme(
      new URI(writeExecutionPlan.writeUri.getOrElse(
        throw new RuntimeException("Entity responded without a writeUri field for a COLD case write"))))
    val spark       = df.sparkSession
    credentialsConfigurator.configureCredentials(writeExecutionPlan, spark.sparkContext.hadoopConfiguration)
    val partitioningColumns: Seq[String] = catalogService.getPartitioningColumns(spark, entityDetails)
    performColdWrite(df, s3path, partitioningColumns, batchId)
    recoverPartitions(spark, partitioningColumns)
  }

  protected def performColdWrite(df: DataFrame, path: URI, partitioningColumns: Seq[String], batchId: Long): Unit

  private def recoverPartitions(sparkSession: SparkSession, partitions: Seq[String]): Unit =
    if (partitions.nonEmpty)
      sparkSession.sql(
        s"""ALTER TABLE ${catalogService.getFullyQualifiedTableName(entityDetails)} RECOVER PARTITIONS"""
      )
}
