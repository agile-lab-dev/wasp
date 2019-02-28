package it.agilelab.bigdata.wasp.yarn.auth.hdfs


import java.net.URI
import java.util.Date
import java.util.regex.Pattern

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.crypto.key.kms.{KMSClientProvider, LoadBalancingKMSClientProvider}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.Master
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier
import org.apache.spark.deploy.yarn.security.ServiceCredentialProvider
import org.apache.spark.internal.Logging
import org.apache.spark.{SparkConf, SparkException}

class HdfsCredentialProvider extends ServiceCredentialProvider with Logging {

  override val serviceName: String = "wasp-hdfs"

  private def getTokenRenewer(conf: Configuration): String = {
    val delegTokenRenewer = Master.getMasterPrincipal(conf)
    logDebug("delegation token renewer is: " + delegTokenRenewer)
    if (delegTokenRenewer == null || delegTokenRenewer.length() == 0) {
      val errorMessage = "Can't get Master Kerberos principal for use as renewer"
      logError(errorMessage)
      throw new SparkException(errorMessage)
    }

    delegTokenRenewer
  }

  override def obtainCredentials(hadoopConf: Configuration, sparkConf: SparkConf, creds: Credentials): Option[Long] = {
    val fact = new KMSClientProvider.Factory()

    val renewer = getTokenRenewer(hadoopConf)


    val hdfsCredentialProviderConfiguration = HdfsCredentialProviderConfiguration.fromSpark(sparkConf)
    logInfo(s"Provider config is: $hdfsCredentialProviderConfiguration")

    val hdfsTokens = hdfsCredentialProviderConfiguration.fs.flatMap {
      fsPath => fsPath.getFileSystem(hadoopConf).addDelegationTokens(renewer, creds)
    }

    val kmsTokens = hdfsCredentialProviderConfiguration.kms.flatMap {
      kmsUri: URI =>

        val provider = fact.createProvider(kmsUri, hadoopConf)

        val didWeGotTheCorrectProvider = provider match {
          case _: LoadBalancingKMSClientProvider => true
          case _: KMSClientProvider => true
          case _ => false
        }


        if (!didWeGotTheCorrectProvider) {
          provider.close()
          throw new Exception(s"The resolved KmsClientProvider is not able to renew delegation tokens, resolved " +
            s"${provider.getClass}")
        }

        try {
          provider match {
            case p: LoadBalancingKMSClientProvider => p.addDelegationTokens(renewer, creds)
            case p: KMSClientProvider => p.addDelegationTokens(renewer, creds)
            case p => throw new Exception(s"unexpected key provider, ${p.getClass.getName}")
          }

        } catch {
          case e: Exception =>
            provider.close()
            throw e
        }
    }

    hdfsTokens.foreach { t =>
      logInfo(s"obtained HDFS delegation token $t")
    }

    kmsTokens.foreach { t =>
      logInfo(s"obtained KMS delegation token $t")
    }


    val maybeNextRenewalDeadline = (hdfsTokens ++ kmsTokens)
      .map(t => t.decodeIdentifier())
      .filter(_.isInstanceOf[AbstractDelegationTokenIdentifier])
      .map(_.asInstanceOf[AbstractDelegationTokenIdentifier])
      .map(_.getMaxDate)
      .reduceOption[Long] {
        case (a, b) if a < b => a
        case (a, b) if b < a => b
        case (a, b) if a == b => a
      }


    maybeNextRenewalDeadline.map(new Date(_)).foreach { deadline =>
      logInfo(s"Final renewal deadline will be $deadline")
    }

    maybeNextRenewalDeadline
  }
}


case class HdfsCredentialProviderConfiguration(kms: Seq[URI], fs: Seq[Path], renew: Long)

object HdfsCredentialProviderConfiguration {

  private val KMS_SEPARATOR_KEY = "spark.wasp.yarn.security.tokens.hdfs.kms.separator"
  private val KMS_SEPARATOR_DEFAULT = "|"
  private val KMS_URIS_KEY = "spark.wasp.yarn.security.tokens.hdfs.kms.uris"
  private val KMS_URIS_VALUE = ""


  private val FS_SEPARATOR_KEY = "wasp.yarn.security.tokens.hdfs.fs.separator"
  private val FS_SEPARATOR_DEFAULT = "|"
  private val FS_URIS_KEY = "spark.wasp.yarn.security.tokens.hdfs.fs.uris"
  private val FS_URIS_VALUE = ""

  private val RENEW_KEY = "spark.wasp.yarn.security.tokens.hdfs.renew"
  private val RENEW_DEFAULT = 600000

  def fromSpark(conf: SparkConf): HdfsCredentialProviderConfiguration = {

    val kmsSeparator = conf.get(KMS_SEPARATOR_KEY, KMS_SEPARATOR_DEFAULT)

    val kmsUris = conf.get(KMS_URIS_KEY, KMS_URIS_VALUE)
      .split(Pattern.quote(kmsSeparator))
      .filterNot(_.isEmpty)
      .map(new URI(_))
      .toVector

    val fsSeparator = conf.get(FS_SEPARATOR_KEY, FS_SEPARATOR_DEFAULT)

    val fsUris = conf.get(FS_URIS_KEY, FS_URIS_VALUE)
      .split(Pattern.quote(fsSeparator))
      .filterNot(_.isEmpty)
      .map(new Path(_))
      .toVector

    val renew = conf.getLong(RENEW_KEY, RENEW_DEFAULT)

    HdfsCredentialProviderConfiguration(kmsUris, fsUris, renew)
  }

  def toSpark(conf: HdfsCredentialProviderConfiguration): SparkConf = {
    val c = new SparkConf()

    c.set(RENEW_KEY, conf.renew.toString)
    c.set(KMS_URIS_KEY, conf.kms.map(_.toString).mkString(KMS_SEPARATOR_DEFAULT))
    c.set(FS_URIS_KEY, conf.fs.map(_.toString).mkString(FS_URIS_VALUE))

  }
}