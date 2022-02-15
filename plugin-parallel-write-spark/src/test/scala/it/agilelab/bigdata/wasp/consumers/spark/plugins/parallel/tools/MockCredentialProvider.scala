package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools

import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider, AWSSessionCredentials}

class MockCredentialProvider extends AWSCredentialsProvider {
  override def getCredentials: AWSCredentials = new AWSSessionCredentials {
    override def getSessionToken: String = "mock"

    override def getAWSAccessKeyId: String = "mock"

    override def getAWSSecretKey: String = "mock"
  }

  override def refresh(): Unit = {}
}
