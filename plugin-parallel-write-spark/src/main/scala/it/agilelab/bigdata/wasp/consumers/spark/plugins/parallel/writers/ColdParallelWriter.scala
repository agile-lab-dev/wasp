package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.writers
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.entity.WriteExecutionPlanResponseBody
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.utils.HadoopS3Utils
import org.apache.spark.sql.{ DataFrame, SparkSession }

import java.net.URI

trait ColdParallelWriter extends ParallelWriter {

  val credentialsConfigurator: CredentialsConfigurator =
    CredentialsConfigurator.coldAreaCredentialsPersisterConfigurator

  final override def write(writeExecutionPlan: WriteExecutionPlanResponseBody, df: DataFrame): Unit = {
    val s3path: URI = HadoopS3Utils.useS3aScheme(new URI(writeExecutionPlan.writeUri))
    val spark       = df.sparkSession
    credentialsConfigurator.configureCredentials(writeExecutionPlan, spark.sparkContext.hadoopConfiguration)
    val partitioningColumns: Seq[String] = catalogService.getPartitioningColumns(spark, entityDetails)
    performColdWrite(df, s3path, partitioningColumns)
    recoverPartitions(spark, partitioningColumns)
  }

  protected def performColdWrite(df: DataFrame, path: URI, partitioningColumns: Seq[String]): Unit

  private def recoverPartitions(sparkSession: SparkSession, partitions: Seq[String]): Unit =
    if (partitions.nonEmpty)
      sparkSession.sql(
        s"""ALTER TABLE ${catalogService.getFullyQualifiedTableName(entityDetails)} RECOVER PARTITIONS"""
      )
}
