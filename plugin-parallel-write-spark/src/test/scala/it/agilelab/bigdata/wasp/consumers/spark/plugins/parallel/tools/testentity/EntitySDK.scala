package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools.testentity

import it.agilelab.bigdata.microservicecatalog.entity.{ParallelWriteEntity, TemporaryCredential, WriteExecutionPlanRequestBody, WriteExecutionPlanResponseBody}
import spray.json.{DefaultJsonProtocol, JsonFormat}
import it.agilelab.bigdata.microservicecatalog.entity.ParallelWriteEntityJsonProtocol._


case class EntitySDK(entityName: String) extends ParallelWriteEntity {
  private def getWriteExecutionPlanEndpoint = getBaseUrl() + "/writeExecutionPlan"
  private def getFlightInfoEndpoint: String = getBaseUrl() + "/flightInfo"
  private def getExecutionPlanEndpoint: String = getBaseUrl() + "/executionPlan"

  import CustomJsonProtocol._

  def getBaseUrl(): String = System.getenv(entityName)
  def getExecutionPlan(source: WriteExecutionPlanRequestBody): ExecutionPlan = call (getExecutionPlanEndpoint, Some(source), Map.empty, "POST")(writeExecutionPlanRequestBodyFormat, ExecutionPlanFormat)
  def getWriteExecutionPlan(): WriteExecutionPlanResponseBody = call(getWriteExecutionPlanEndpoint, Some(WriteExecutionPlanRequestBody(source="Self")), Map.empty, "POST")(writeExecutionPlanRequestBodyFormat, writeExecutionPlanResponseBodyFormat)
  def getFlightInfo(): FlightInfo = call(getFlightInfoEndpoint, None, Map.empty, "GET")(writeExecutionPlanRequestBodyFormat, FlightInfoFormat)
}

object CustomJsonProtocol extends DefaultJsonProtocol {
  implicit val ExecutionPlanFormat: JsonFormat[ExecutionPlan] = jsonFormat3(ExecutionPlan)
  implicit val PartitionFormat: JsonFormat[Partition] = jsonFormat2(Partition)
  implicit val FlightInfoFormat: JsonFormat[FlightInfo] = jsonFormat2(FlightInfo)
}

final case class ExecutionPlan(extractionType: String, temporaryCredential: Option[TemporaryCredential], s3Path: Option[String])

// Flight info
final case class FlightInfo(filterApplied: Boolean, partitions: List[Partition])
final case class Partition(id: Int, partitionFilter: String)

// Complete
final case class Complete(success: Boolean)
