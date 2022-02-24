package it.agilelab.bigdata.wasp.yarn.auth.hdfs

import java.net.URI
import java.nio.file.Files

import it.agilelab.bigdata.wasp.yarn.auth.hdfs.HdfsCredentialProviderConfiguration.toSpark
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.crypto.key.kms.server.MiniKMS
import org.apache.hadoop.security.Credentials
import org.scalatest.{Matchers, WordSpecLike}

import scala.collection.JavaConverters._

class KmsTest extends WordSpecLike with Matchers {

  "An HdfsCredentialProvider running inside the Application Master" must {

    "Renew KMS delegation tokens" in {

      val builder = new MiniKMS.Builder()

      val dir = Files.createTempDirectory("kms")

      val kms = builder.setKmsConfDir(dir.toFile).build()

      kms.start()

      val myKmsUrl = new URI(kms.getKMSUrl.toString.replace("http://", "kms://http@"))

      val prov = new HdfsCredentialProvider

      val hadoopConf = new Configuration

      hadoopConf.set("yarn.resourcemanager.principal", "yarn")

      val sparkConf = toSpark(HdfsCredentialProviderConfiguration(Seq(myKmsUrl), Seq.empty, 60000))

      val credentials = new Credentials()

      prov.obtainCredentials(hadoopConf, sparkConf, credentials)

      val tokens = credentials.getAllTokens.asScala.toSeq

      tokens should have size 1

      tokens.head.getKind.toString should be ("kms-dt")

      kms.stop()

    }
  }
}
