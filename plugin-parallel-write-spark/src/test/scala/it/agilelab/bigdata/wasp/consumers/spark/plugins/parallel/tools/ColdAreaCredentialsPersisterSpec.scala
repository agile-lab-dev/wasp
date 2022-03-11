package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools

import com.amazonaws.auth.{AWSCredentials, AWSSessionCredentials}
import it.agilelab.bigdata.wasp.aws.auth.v2.{ConfigurationLoader, CredentialsSerde}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.entity._
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools.utils.TempDirectoryTest
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.utils.HadoopS3Utils
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.writers.ColdAreaCredentialsPersister
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.FunSuite

import java.net.URI

class ColdAreaCredentialsPersisterSpec extends FunSuite with TempDirectoryTest {

  lazy val configuration = ColdAreaCredentialsPersisterSpec.buildMockConfiguration()

  test("Save credentials") {
    val writeExecutionPlanResponseBody: WriteExecutionPlanResponseBody =
      WriteExecutionPlanResponseBody(
        "Parquet",
        "file://bucket/dir",
        "Cold",
        TemporaryCredentials(
          r = TemporaryCredential("ReadAccessKey", "ReadSecretKey", "ReadToken"),
          w = TemporaryCredential("WriteAccessKey", "WriteSecretKey", "WriteToken")
        ))

    ColdAreaCredentialsPersister.writeCredentials(writeExecutionPlanResponseBody, configuration)
    checkCredentials(writeExecutionPlanResponseBody)
  }

  test("Save multiple buckets credentials") {
    val bucket1 = "file://bucket1/dir"
    val bucket2 = "file://bucket2/dir"
    val writeExecutionPlanResponseBody1: WriteExecutionPlanResponseBody =
      WriteExecutionPlanResponseBody(
        "Parquet",
        bucket1,
        "Cold",
        TemporaryCredentials(
          r = TemporaryCredential("ReadAccessKey1", "ReadSecretKey1", "ReadToken1"),
          w = TemporaryCredential("WriteAccessKey1", "WriteSecretKey1", "WriteToken1")
        ))
    val writeExecutionPlanResponseBody2: WriteExecutionPlanResponseBody =
      WriteExecutionPlanResponseBody(
        "Parquet",
        bucket2,
        "Cold",
        TemporaryCredentials(
          r = TemporaryCredential("ReadAccessKey2", "ReadSecretKey2", "ReadToken2"),
          w = TemporaryCredential("WriteAccessKey2", "WriteSecretKey2", "WriteToken2")
        ))

    ColdAreaCredentialsPersister.writeCredentials(writeExecutionPlanResponseBody1, configuration)
    ColdAreaCredentialsPersister.writeCredentials(writeExecutionPlanResponseBody2, configuration)
    checkCredentials(writeExecutionPlanResponseBody1)
    checkCredentials(writeExecutionPlanResponseBody2)
  }

  private def checkCredentials(writeExecutionPlanResponseBody: WriteExecutionPlanResponseBody) = {
    val readedCredentials: AWSCredentials = readCredentials(writeExecutionPlanResponseBody.writeUri)
    assert(readedCredentials.isInstanceOf[AWSSessionCredentials])
    val readedSessionCredentials = readedCredentials.asInstanceOf[AWSSessionCredentials]
    assert(readedSessionCredentials.getAWSAccessKeyId == writeExecutionPlanResponseBody.temporaryCredentials.w.accessKeyID)
    assert(readedSessionCredentials.getAWSSecretKey == writeExecutionPlanResponseBody.temporaryCredentials.w.secretKey)
    assert(readedSessionCredentials.getSessionToken == writeExecutionPlanResponseBody.temporaryCredentials.w.sessionToken)
  }

  private def readCredentials(writeUri: String): AWSCredentials = {
    val uri = HadoopS3Utils.useS3aScheme(new URI(writeUri))
    val conf = ConfigurationLoader.lookupConfig(uri, configuration)
    val path = new Path(conf.getStoragePath, conf.getBucket.getHost)
    val fs = path.getFileSystem(conf.getConfiguration)
    CredentialsSerde.read(fs, path)
  }
}

object ColdAreaCredentialsPersisterSpec extends ColdAreaCredentialsPersisterSpec {
  private def buildMockConfiguration():Configuration = {
    val conf = new Configuration()
    conf.set("fs.s3a.aws.credentials.provider" , "it.agilelab.bigdata.wasp.aws.auth.v2.PlacementAwareCredentialsProvider")
    conf.set("fs.s3a.assumed.role.credentials.provider" , "it.agilelab.bigdata.wasp.aws.auth.v2.WebIdentityProvider")
    conf.set("it.agilelab.bigdata.wasp.aws.auth.storage" , tempDir)
    conf.set("it.agilelab.bigdata.wasp.aws.auth.renewmillis" , "60000")
    conf.set("it.agilelab.bigdata.wasp.aws.auth.delegate" , classOf[MockCredentialProvider].getCanonicalName)
    conf
  }
}