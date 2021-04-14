package it.agilelab.bigdata.wasp.consumers.spark.plugins.continuous.tools

import cloud.localstack.Localstack
import cloud.localstack.awssdkv1.TestUtils
import cloud.localstack.docker.annotation.{LocalstackDockerAnnotationProcessor, LocalstackDockerProperties}
import it.agilelab.bigdata.wasp.consumers.spark.utils.SparkSuite
import org.scalatest.{Args, BeforeAndAfterEach, Status, Suite};


@LocalstackDockerProperties(services = Array("s3"))
trait TempDirectoryEach extends BeforeAndAfterEach with SparkSuite { self: Suite =>

  protected def tempDir: String = "tmpbucket"
  override def beforeEach(): Unit = {
    super.beforeEach()
    spark.sql("CREATE TABLE IF NOT EXISTS test_table (column1 STRING, column2 STRING) STORED AS PARQUET")
    val config = LocalstackSpec.processor.process(this.getClass)
    Localstack.INSTANCE.startup(config)
    TestUtils.getClientS3.createBucket(tempDir)
  }

  override def afterEach(): Unit = {
    spark.sql("DROP TABLE test_table")
    try {
      Localstack.INSTANCE.stop()
    } finally {
      super.afterEach()
    }
  }

  override abstract def runTest(testName: String, args: Args): Status = super[BeforeAndAfterEach].runTest(testName, args)
}

object LocalstackSpec {
  val processor = new LocalstackDockerAnnotationProcessor
}