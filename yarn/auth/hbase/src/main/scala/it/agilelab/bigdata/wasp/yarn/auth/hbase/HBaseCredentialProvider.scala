package it.agilelab.bigdata.wasp.yarn.auth.hbase

import java.sql.Date
import java.util.regex.Pattern

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.security.token.{TokenUtil, AuthenticationTokenIdentifier => HbaseTokenIdentifier}
import org.apache.hadoop.security.Credentials
import org.apache.spark.SparkConf
import org.apache.spark.deploy.yarn.security.ServiceCredentialProvider
import org.apache.spark.internal.Logging

class HBaseCredentialsProvider extends ServiceCredentialProvider with Logging {

  import HBaseWaspCredentialsProvider._

  override def serviceName: String = "wasp-hbase"

  override def obtainCredentials(hadoopConf: Configuration, sparkConf: SparkConf, creds: Credentials): Option[Long] = {

    val providerConfig = HbaseCredentialsProviderConfiguration.fromSpark(sparkConf)

    logInfo(s"Provider config is: $providerConfig")

    val hbaseConf = HbaseCredentialsProviderConfiguration.toHbaseConf(providerConfig)

    try {
      logInfo("Renewing token")
      //this method is deprecaded because it opens and closes a new connection
      //we want this behavior in order to be able to renew tokens
      val token = TokenUtil.obtainToken(hbaseConf)

      val tokenIdentifier = token.decodeIdentifier()

      creds.addToken(token.getService, token)

      logInfo(s"Token renewed ${stringifyToken(tokenIdentifier)}")

    } catch {
      case e: Exception =>
        //this exception is catched here and not rethrown because spark will catch it and then abort renewal for
        // 1HOUR, we should abort but its currently not clear how to do this from the application master
        logError("Something went really bad while authenticating via hbase", e)
    }

    //renewal is not supported, another token should be obtained
    None
  }

  override def credentialsRequired(sparkConf: SparkConf, hadoopConf: Configuration): Boolean = super.credentialsRequired(sparkConf, hadoopConf)
}

object HBaseWaspCredentialsProvider {


  def stringifyToken(tokenIdentifier: HbaseTokenIdentifier): String = {
    Seq(
      ("Username", tokenIdentifier.getUsername),
      ("SequenceNumber", tokenIdentifier.getSequenceNumber),
      ("KeyId", tokenIdentifier.getKeyId),
      ("IssueDate", new Date(tokenIdentifier.getIssueDate)),
      ("ExpirationDate", new Date(tokenIdentifier.getExpirationDate))
    ).map {
      case (name, value) => s"$name=$value"
    }.mkString(", ")
  }

}

case class HbaseCredentialsProviderConfiguration(configurationFiles: Seq[Path],
                                                 failFast: Boolean,
                                                 other: Seq[(String, String)])


object HbaseCredentialsProviderConfiguration {

  private val HADOOP_CONF_TO_LOAD_KEY = "spark.wasp.yarn.security.tokens.hbase.config.files"
  private val HADOOP_CONF_TO_LOAD_DEFAULT = ""
  private val HADOOP_CONF_TO_LOAD_SEPARATOR_KEY = "spark.wasp.yarn.security.tokens.hbase.config.separator"
  private val HADOOP_CONF_TO_LOAD_SEPARATOR_DEFAULT = "|"
  private val HADOOP_CONF_TO_LOAD_INLINE_PREFIX = "spark.wasp.yarn.security.tokens.hbase.config.inline"
  private val HADOOP_CONF_FAILFAST_KEY = "spark.wasp.yarn.security.tokens.hbase.failfast"
  private val HADOOP_CONF_FAILFAST_DEFAULT = true

  def fromSpark(conf: SparkConf): HbaseCredentialsProviderConfiguration = {

    val actualSeparator = conf.get(HADOOP_CONF_TO_LOAD_SEPARATOR_KEY, HADOOP_CONF_TO_LOAD_SEPARATOR_DEFAULT)

    val filesToLoad = conf.get(HADOOP_CONF_TO_LOAD_KEY, HADOOP_CONF_TO_LOAD_DEFAULT)
      .split(Pattern.quote(actualSeparator))
      .filterNot(_.isEmpty)
      .map(new Path(_))

    val other = conf.getAllWithPrefix(HADOOP_CONF_TO_LOAD_INLINE_PREFIX)

    val failFast = conf.getBoolean(HADOOP_CONF_FAILFAST_KEY, HADOOP_CONF_FAILFAST_DEFAULT)

    HbaseCredentialsProviderConfiguration(filesToLoad, failFast, other)
  }


  def toHbaseConf(conf: HbaseCredentialsProviderConfiguration): Configuration = {
    val hbaseConfig = HBaseConfiguration.create()

    conf.configurationFiles.foreach(hbaseConfig.addResource)
    conf.other.foreach {
      case (k, v) => hbaseConfig.set(k, v)
    }

    if (conf.failFast) {
      hbaseConfig.set("hbase.client.retries.number ", "1")
    }

    hbaseConfig
  }
}