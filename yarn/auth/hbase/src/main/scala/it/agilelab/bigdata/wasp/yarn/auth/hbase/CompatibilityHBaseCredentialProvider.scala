package it.agilelab.bigdata.wasp.yarn.auth.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.Credentials
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.security.HadoopDelegationTokenProvider

import scala.reflect.runtime.universe
import scala.util.control.NonFatal

trait CompatibilityHBaseCredentialProvider extends HadoopDelegationTokenProvider  with Logging {

  self: HBaseCredentialsProvider =>
  override def obtainDelegationTokens(hadoopConf: Configuration, sparkConf: SparkConf, creds: Credentials): Option[Long] = {
    getCredentials(hadoopConf, sparkConf, creds)
  }

  override def delegationTokensRequired(
                                         sparkConf: SparkConf,
                                         hadoopConf: Configuration): Boolean = {
    hbaseConf(hadoopConf).get("hbase.security.authentication") == "kerberos"
  }

  private def hbaseConf(conf: Configuration): Configuration = {
    try {
      val mirror = universe.runtimeMirror(getContextOrSparkClassLoader)
      val confCreate = mirror.classLoader.
        loadClass("org.apache.hadoop.hbase.HBaseConfiguration").
        getMethod("create", classOf[Configuration])
      confCreate.invoke(null, conf).asInstanceOf[Configuration]
    } catch {
      case NonFatal(e) =>
        logWarning("Fail to invoke HBaseConfiguration", e)
        conf
    }
  }

  def getContextOrSparkClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getClass.getClassLoader)

}
