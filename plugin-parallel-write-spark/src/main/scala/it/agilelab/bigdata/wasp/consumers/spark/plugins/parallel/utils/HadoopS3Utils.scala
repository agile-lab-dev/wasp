package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.utils

import akka.http.scaladsl.model.Uri
import it.agilelab.bigdata.microservicecatalog.entity
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.utils.HadoopS3Utils.values
import org.apache.hadoop.conf.Configuration

import scala.util.Try

object HadoopS3Utils {
  def values(credentials: entity.TemporaryCredential): Seq[(String, String)] =
    Seq(
      "fs.s3a.impl" -> "org.apache.hadoop.fs.s3a.S3AFileSystem",
      "fs.s3a.path.style.access" -> "true",
      "fs.s3a.aws.credentials.provider" -> "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
      "fs.s3a.access.key" -> credentials.accessKeyID,
      "fs.s3a.secret.key" -> credentials.secretKey,
      "fs.s3a.session.token" -> credentials.sessionToken
    )

  def useS3aScheme(writeUri: String): Uri = {
    if (Uri(writeUri).scheme == "s3")
      Uri(writeUri).withScheme("s3a")
    else writeUri
  }
}

class HadoopS3aUtil(hadoopCfg: Configuration, credentials: entity.TemporaryCredential) {

  def performBulkHadoopCfgSetup: Try[Unit] =
    Try(values(credentials).foreach { case (key, value) => hadoopCfg.set(key, value) })
}