package it.agilelab.bigdata.wasp.yarn.auth.hdfs


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.security.HadoopDelegationTokenProvider

trait CompatibilityHadoopDelegationTokenProvider extends HadoopDelegationTokenProvider with Logging {

  self: HdfsCredentialProvider =>
  override def obtainDelegationTokens(hadoopConf: Configuration, sparkConf: SparkConf, creds: Credentials): Option[Long] = {
    getDelegationTokens(hadoopConf, sparkConf, creds)
  }

  override def delegationTokensRequired(
                                         sparkConf: SparkConf,
                                         hadoopConf: Configuration): Boolean = {
    UserGroupInformation.isSecurityEnabled
  }


}



