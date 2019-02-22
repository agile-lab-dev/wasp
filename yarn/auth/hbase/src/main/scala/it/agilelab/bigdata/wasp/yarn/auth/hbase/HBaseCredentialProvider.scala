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

    val hbaseConf = createHBaseConfFromSparkConf(sparkConf)

    try {

      logInfo("Renewing token")

      val token = TokenUtil.obtainToken(hbaseConf)

      val tokenIdentifier = token.asInstanceOf[HbaseTokenIdentifier]

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
  val HADOOP_CONF_TO_LOAD_KEY = "wasp.yarn.security.tokens.hbase.config.files"
  val HADOOP_CONF_TO_LOAD_KEY_DEFAULT = ""
  val HADOOP_CONF_TO_LOAD_SEPARATOR_KEY = "wasp.yarn.security.tokens.hbase.config.separator"
  val HADOOP_CONF_TO_LOAD_SEPARATOR_DEFAULT = "|"
  val HADOOP_CONF_TO_LOAD_INLINE_PREFIX = "wasp.yarn.security.tokens.hbase.config.inline"
  val HADOOP_CONF_FAILFAST_KEY = "wasp.yarn.security.tokens.hbase.failfast"
  val HADOOP_CONF_FAILFAST_DEFAULT = true


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

  def createHBaseConfFromSparkConf(sparkConf: SparkConf): Configuration = {

    val config = HBaseConfiguration.create()

    val actualSeparator = sparkConf.get(HADOOP_CONF_TO_LOAD_SEPARATOR_KEY, HADOOP_CONF_TO_LOAD_SEPARATOR_DEFAULT)

    val filesToLoad = sparkConf.get(HADOOP_CONF_TO_LOAD_KEY, HADOOP_CONF_TO_LOAD_KEY_DEFAULT)
      .split(Pattern.quote(actualSeparator))
      .filterNot(_.isEmpty)

    filesToLoad.foreach { f =>
      config.addResource(new Path(f))
    }

    sparkConf.getAllWithPrefix(HADOOP_CONF_TO_LOAD_INLINE_PREFIX).foreach {
      case (k, v) => config.set(k, v)
    }

    if (sparkConf.getBoolean(HADOOP_CONF_FAILFAST_KEY, HADOOP_CONF_FAILFAST_DEFAULT)) {
      config.set("hbase.client.retries.number ", "1")
    }

    config
  }

}
