package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.writers

import it.agilelab.bigdata.wasp.aws.auth.v2.{ConfigurationLoader, CredentialsSerde}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.entity._
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.utils.HadoopS3Utils
import it.agilelab.bigdata.wasp.core.logging.Logging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import java.net.URI
import java.time.Instant

object ColdAreaCredentialsPersister extends CredentialsConfigurator with Logging {

  def writeCredentials(writeExecutionPlanResponseBody: WriteExecutionPlanResponseBody, configuration: Configuration) = {
    val awsWriteCredentials = writeExecutionPlanResponseBody.temporaryCredentials.w.toAWSSessionCredentials()
    val bucketTokenPath = computeBucketTokenPath(writeExecutionPlanResponseBody, configuration)
    logger.info("Cleaning old credentials")
    CredentialsSerde.cleanupOldCredentials(configuration, bucketTokenPath)
    logger.info("Old credentials cleaned")
    logger.info("Writing credentials")
    CredentialsSerde.write(configuration, bucketTokenPath, awsWriteCredentials, getFileName())
    logger.info("Wrote credentials")
  }

  private def getFileName(): String = {
    val maxLongAsString: String = Long.MaxValue.toString
    val now: Long = Instant.now().toEpochMilli
    val paddedFileName = s"%0${maxLongAsString.length}d".format(now)
    paddedFileName
  }

  private def computeBucketTokenPath(writeExecutionPlanResponseBody: WriteExecutionPlanResponseBody, configuration: Configuration) = {
    val uri = HadoopS3Utils.useS3aScheme(new URI(writeExecutionPlanResponseBody.writeUri))
    val conf = ConfigurationLoader.lookupConfig(uri, configuration)
    val host = Option(conf.getBucket.getHost).getOrElse("file-bucket")
    new Path(conf.getStoragePath(), host);
  }

  override def configureCredentials(writeExecutionPlanResponseBody: WriteExecutionPlanResponseBody, configuration: Configuration): Unit = {
    writeCredentials(writeExecutionPlanResponseBody, configuration)
  }
}
