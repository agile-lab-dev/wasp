package it.agilelab.bigdata.microservicecatalog.tools.builders.microservices

import it.agilelab.bigdata.microservicecatalog.entity.{ParallelWriteEntity, TemporaryCredential, WriteExecutionPlanRequestBody, WriteExecutionPlanResponseBody}


case class EntitySDK(entityName: String) extends ParallelWriteEntity {
  private def getWriteExecutionPlanEndpoint() = getBaseUrl() + "/writeExecutionPlan"
  private def getFlightInfoEndpoint(): String = getBaseUrl() + "/flightInfo"
  private def getExecutionPlanEndpoint(): String = getBaseUrl() + "/executionPlan"


  def getBaseUrl() = System.getenv(entityName)
  def getExecutionPlan(source: WriteExecutionPlanRequestBody): ExecutionPlan = call (getExecutionPlanEndpoint(), Some(source), Map.empty, "POST")(implicitly[Manifest[ExecutionPlan]])
  def getWriteExecutionPlan(source: WriteExecutionPlanRequestBody): WriteExecutionPlanResponseBody = call(getWriteExecutionPlanEndpoint(), Some(source), Map.empty, "POST")(implicitly[Manifest[WriteExecutionPlanResponseBody]])
  def getFlightInfo(): FlightInfo = call(getFlightInfoEndpoint, None, Map.empty, "GET")(implicitly[Manifest[FlightInfo]])
}


object EntitySDK extends EntitySDK("mock") {
  override def getBaseUrl(): String = "http://localhost:9999"
}

final case class ExecutionPlan(extractionType: String, temporaryCredential: Option[TemporaryCredential], s3Path: Option[String])

// Flight info
final case class FlightInfo(filterApplied: Boolean, partitions: List[Partition])
final case class Partition(id: Int, partitionFilter: String)


// Complete
final case class Complete(success: Boolean)
