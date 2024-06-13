package it.agilelab.bigdata.wasp.yarn.auth.hdfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.Credentials
import org.apache.spark.deploy.yarn.security.ServiceCredentialProvider
import org.apache.spark.internal.Logging
import org.apache.spark.SparkConf

trait CompatibilityHadoopDelegationTokenProvider extends ServiceCredentialProvider with Logging {
  self: HdfsCredentialProvider =>
  override def obtainCredentials(hadoopConf: Configuration, sparkConf: SparkConf, creds: Credentials): Option[Long] = {
    getDelegationTokens(hadoopConf, sparkConf, creds)
  }
}
