package it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.microservicecatalog

import it.agilelab.bigdata.microservicecatalog.entity.{ParallelWriteEntity, TemporaryCredential, WriteExecutionPlanRequestBody, WriteExecutionPlanResponseBody}


case class EntitySDK(entityName: String) extends ParallelWriteEntity {
  private def getWriteExecutionPlanEndpoint() = getBaseUrl() + "/writeExecutionPlan"


  def getBaseUrl() = System.getenv(entityName)
  def getWriteExecutionPlan(): WriteExecutionPlanResponseBody = call(getWriteExecutionPlanEndpoint(), Some(WriteExecutionPlanRequestBody(source="Self")), Map.empty, "POST")(implicitly[Manifest[WriteExecutionPlanResponseBody]])
}

