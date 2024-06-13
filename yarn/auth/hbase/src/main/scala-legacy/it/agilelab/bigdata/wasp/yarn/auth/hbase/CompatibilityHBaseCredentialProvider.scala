package it.agilelab.bigdata.wasp.yarn.auth.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.Credentials
import org.apache.spark.SparkConf
import org.apache.spark.deploy.yarn.security.ServiceCredentialProvider
import org.apache.spark.internal.Logging

trait CompatibilityHBaseCredentialProvider extends ServiceCredentialProvider with Logging {

  self: HBaseCredentialsProvider =>
  @com.github.ghik.silencer.silent("deprecated")
  override def obtainCredentials(hadoopConf: Configuration, sparkConf: SparkConf, creds: Credentials): Option[Long] = {
    getCredentials(hadoopConf, sparkConf, creds)
  }

  override def credentialsRequired(hadoopConf: Configuration): Boolean = super.credentialsRequired(hadoopConf)
}
