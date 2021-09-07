package it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.microservicecatalog

import it.agilelab.bigdata.microservicecatalog.entity.{ParallelWriteEntity, WriteExecutionPlanRequestBody, WriteExecutionPlanResponseBody}
import it.agilelab.bigdata.microservicecatalog.entity.ParallelWriteEntityJsonProtocol._


case class EntitySDK(entityName: String) extends ParallelWriteEntity {
  private def getWriteExecutionPlanEndpoint: String = getBaseUrl() + "/writeExecutionPlan"


  def getBaseUrl(): String = System.getenv(entityName)
  def getWriteExecutionPlan(): WriteExecutionPlanResponseBody = call(getWriteExecutionPlanEndpoint, Some(WriteExecutionPlanRequestBody(source="Self")), Map.empty, "POST")(writeExecutionPlanRequestBodyFormat, writeExecutionPlanResponseBodyFormat)
}

