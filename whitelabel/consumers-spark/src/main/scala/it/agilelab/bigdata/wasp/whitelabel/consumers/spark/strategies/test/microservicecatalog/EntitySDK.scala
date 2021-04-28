package it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.microservicecatalog

import it.agilelab.bigdata.microservicecatalog.entity.{ParallelWriteEntity, TemporaryCredential, WriteExecutionPlanRequestBody, WriteExecutionPlanResponseBody}


case class EntitySDK(entityName: String) extends ParallelWriteEntity {
  private def getWriteExecutionPlanEndpoint() = getBaseUrl() + "/writeExecutionPlan"


  def getBaseUrl() = System.getenv(entityName)
  def getWriteExecutionPlan(source: WriteExecutionPlanRequestBody): WriteExecutionPlanResponseBody = call(getWriteExecutionPlanEndpoint(), Some(source), Map.empty, "POST")(implicitly[Manifest[WriteExecutionPlanResponseBody]])
}

