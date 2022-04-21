package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.entity

import com.amazonaws.auth.{AWSSessionCredentials, BasicSessionCredentials}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.MicroserviceClient
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.entity.ParallelWriteEntity.CorrelationId
import spray.json.{DefaultJsonProtocol, JsonFormat}

import java.util.UUID

/**
  * An entity supporting parallel write use case
  */
trait ParallelWriteEntity extends MicroserviceClient {
  def getWriteExecutionPlan(
      requestBody: WriteExecutionPlanRequestBody,
      correlationId: CorrelationId
  ): WriteExecutionPlanResponseBody
  def postDataStream(requestBody: DataStreamRequestBody, correlationId: CorrelationId): Unit
  def postDataComplete(requestBody: DataCompleteRequestBody, correlationId: CorrelationId): Unit
  def getDataCommitted(correlationId: CorrelationId): DataCommittedResponseBody
}

object ParallelWriteEntity {
  class CorrelationId(val value: String) extends AnyVal
  def randomCorrelationId(): CorrelationId = new CorrelationId(UUID.randomUUID().toString)
}

// Parallel write
object ParallelWriteFormat extends Enumeration {
  type ParallelWriteFormat = Value
  val parquet = Value("Parquet")
  val delta   = Value("Delta")
  val hot     = Value("Hot")
}

object ParallelWriteEntityJsonProtocol extends DefaultJsonProtocol {
  implicit val temporaryCredentialFormat: JsonFormat[TemporaryCredential]   = jsonFormat3(TemporaryCredential)
  implicit val temporaryCredentialsFormat: JsonFormat[TemporaryCredentials] = jsonFormat2(TemporaryCredentials)
  implicit val writeExecutionPlanRequestBodyFormat: JsonFormat[WriteExecutionPlanRequestBody] = jsonFormat2(
    WriteExecutionPlanRequestBody
  )
  implicit val writeExecutionPlanResponseBodyFormat: JsonFormat[WriteExecutionPlanResponseBody] = jsonFormat(
    WriteExecutionPlanResponseBody, "format", "writeUri", "writeType", "temporaryCredentials"
  )
  implicit val dataCompleteRequestBodyFormat: JsonFormat[DataCompleteRequestBody] = jsonFormat1(DataCompleteRequestBody)
  implicit val dataCommittedResponseBodyFormat: JsonFormat[DataCommittedResponseBody] = jsonFormat1(
    DataCommittedResponseBody
  )

}

case class WriteExecutionPlanResponseBody(
    format: Option[String],
    writeUri: Option[String],
    writeType: String,
    temporaryCredentials: Option[TemporaryCredentials]
)
case class WriteExecutionPlanRequestBody(
    applicationFilter: Option[String] = None,
    dataGovernanceFilter: Option[String] = None
)
case class DataStreamRequestBody(stream: Iterator[String])
case class DataCompleteRequestBody(success: Boolean)
case class DataCommittedResponseBody(commitStatus: String)
case class TemporaryCredentials(r: TemporaryCredential, w: TemporaryCredential)

case class TemporaryCredential(accessKeyID: String, secretKey: String, sessionToken: String) {
  def toAWSSessionCredentials(): AWSSessionCredentials =
    new BasicSessionCredentials(accessKeyID, secretKey, sessionToken)
}
