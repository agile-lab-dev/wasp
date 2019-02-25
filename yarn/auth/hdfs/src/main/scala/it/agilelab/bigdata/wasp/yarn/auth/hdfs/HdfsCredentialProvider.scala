package it.agilelab.bigdata.wasp.yarn.auth.hdfs


import java.net.URI
import java.util.regex.Pattern

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.crypto.key.KeyProviderDelegationTokenExtension
import org.apache.hadoop.crypto.key.kms.KMSClientProvider
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.Master
import org.apache.hadoop.security.Credentials
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

    hdfsCredentialProviderConfiguration.fs.foreach {
      fsPath =>
        val obtained = fsPath.getFileSystem(hadoopConf).addDelegationTokens(renewer, creds)
        obtained.foreach(t => logInfo(s"token: $t"))
    }

    hdfsCredentialProviderConfiguration.kms.foreach {
      kmsUri: URI =>

        val provider = fact.createProvider(kmsUri, hadoopConf)

        val didWeGotTheCorrectProvider = provider.isInstanceOf[KeyProviderDelegationTokenExtension.DelegationTokenExtension]

        if (!didWeGotTheCorrectProvider) {
          provider.close()
          throw new Exception(s"The resolved KmsClientProvider is not able to renew delegation tokens, resolved " +
            s"${provider.getClass}")
        }

        try {
          val ableToRenewProvider = provider.asInstanceOf[KeyProviderDelegationTokenExtension.DelegationTokenExtension]


          val obtained = ableToRenewProvider.addDelegationTokens(renewer, creds)

          obtained.foreach(t => logInfo(s"token: $t"))
        } catch {
          case e: Exception =>
            provider.close()
            throw e
        }
    }
    Some(hdfsCredentialProviderConfiguration.renew)
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
}