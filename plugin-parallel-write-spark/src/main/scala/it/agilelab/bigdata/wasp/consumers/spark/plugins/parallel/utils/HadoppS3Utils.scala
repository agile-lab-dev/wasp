package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.utils

import akka.http.scaladsl.model.Uri
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.TemporaryCredential
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.utils.HadoppS3Utils.values
import org.apache.hadoop.conf.Configuration

import scala.util.Try

object HadoppS3Utils {
  def values(credentials: TemporaryCredential): Seq[(String, String)] =
    Seq(
      "fs.s3a.impl" -> "org.apache.hadoop.fs.s3a.S3AFileSystem",
      "fs.s3a.endpoint" -> "host.docker.internal:4566", //"s3.eu-east-1.amazonaws.com",
      "fs.s3a.path.style.access" -> "true",
      "fs.s3a.connection.ssl.enabled" -> "false",
      "fs.s3a.aws.credentials.provider" -> "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
      "fs.s3a.access.key" -> credentials.accessKeyID,
      "fs.s3a.secret.key" -> credentials.secretKey,
      "fs.s3a.session.token" -> credentials.sessionToken
    )

  def useS3aScheme(writeUri: String): Uri = {
    Uri(writeUri).withScheme("s3a")
  }
}

class HadoopS3aUtil(hadoopCfg: Configuration, credentials: TemporaryCredential) {

  def performBulkHadoopCfgSetup: Try[Unit] =
    Try(values(credentials).foreach { case (key, value) => hadoopCfg.set(key, value) })
}