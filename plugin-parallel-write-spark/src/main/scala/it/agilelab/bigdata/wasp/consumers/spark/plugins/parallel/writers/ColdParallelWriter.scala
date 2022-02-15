package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.writers
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.entity.WriteExecutionPlanResponseBody
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.utils.{GlueDataCatalogService, HadoopS3Utils}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.net.URI

trait ColdParallelWriter extends ParallelWriter {

  val credentialsConfigurator: CredentialsConfigurator = CredentialsConfigurator.coldAreaCredentialsPersisterConfigurator

  override final def write(writeExecutionPlan: WriteExecutionPlanResponseBody, df: DataFrame): Unit = {
    val s3path: URI = HadoopS3Utils.useS3aScheme(new URI(writeExecutionPlan.writeUri))
    credentialsConfigurator.configureCredentials(writeExecutionPlan, df.sparkSession.sparkContext.hadoopConfiguration)
    val partitioningColumns: Seq[String] = catalogService.getPartitioningColumns(df.sparkSession, entityDetails)
    performColdWrite(df, s3path, partitioningColumns)
    recoverPartitions(df.sparkSession, partitioningColumns)
  }

  protected def performColdWrite(df: DataFrame, path: URI, partitioningColumns: Seq[String]): Unit

  private def recoverPartitions(sparkSession: SparkSession, partitions: Seq[String]): Unit = {
    if (partitions.nonEmpty)
      sparkSession.sql(s"""ALTER TABLE ${catalogService.getFullyQualifiedTableName(entityDetails)} RECOVER PARTITIONS""")
  }
}
