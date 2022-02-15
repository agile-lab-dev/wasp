package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.entity

import com.amazonaws.auth.{AWSSessionCredentials, BasicSessionCredentials}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.MicroserviceClient
import spray.json.{DefaultJsonProtocol, JsonFormat}

/**
  * An entity supporting parallel write use case
  */
trait ParallelWriteEntity extends MicroserviceClient {
  /**
    * Gets the execution plan containing information about s3 path and temporary credentials
    * @param source
    * @return
    */
  def getWriteExecutionPlan(source: WriteExecutionPlanRequestBody): WriteExecutionPlanResponseBody
}

// Parallel write
object ParallelWriteFormat extends Enumeration {
  type ParallelWriteFormat = Value
  val parquet = Value("Parquet")
  val delta = Value("Delta")
}

object ParallelWriteEntityJsonProtocol extends DefaultJsonProtocol {
  implicit val temporaryCredentialFormat: JsonFormat[TemporaryCredential] = jsonFormat3(TemporaryCredential)
  implicit val temporaryCredentialsFormat: JsonFormat[TemporaryCredentials] = jsonFormat2(TemporaryCredentials)
  implicit val writeExecutionPlanRequestBodyFormat: JsonFormat[WriteExecutionPlanRequestBody] = jsonFormat3(WriteExecutionPlanRequestBody)
  implicit val writeExecutionPlanResponseBodyFormat: JsonFormat[WriteExecutionPlanResponseBody] = jsonFormat4(WriteExecutionPlanResponseBody)
}

case class WriteExecutionPlanResponseBody(format: String, writeUri: String, writeType: String, temporaryCredentials: TemporaryCredentials)
case class WriteExecutionPlanRequestBody(source: String, applicationFilter: Option[String] = None, dataGovernanceFilter: Option[String] = None)
case class TemporaryCredentials(r: TemporaryCredential, w: TemporaryCredential)

case class TemporaryCredential(accessKeyID: String, secretKey: String, sessionToken: String) {
  def toAWSSessionCredentials(): AWSSessionCredentials = new BasicSessionCredentials(
    accessKeyID,
    secretKey,
    sessionToken)
}
