package it.agilelab.bigdata.wasp.whitelabel.consumers.spark.aws
import com.amazonaws.auth.{AWSCredentials, BasicAWSCredentials}
import org.apache.hadoop.conf.Configuration

import java.net.URI

class MinioCredentialsProvider(uri: URI, configuration: Configuration)
    extends com.amazonaws.auth.AWSCredentialsProvider {
  override def getCredentials: AWSCredentials = new BasicAWSCredentials(
    sys.env("MINIO_ROOT_USER"),
    sys.env("MINIO_ROOT_PASSWORD")
  )

  override def refresh(): Unit = {}
}
