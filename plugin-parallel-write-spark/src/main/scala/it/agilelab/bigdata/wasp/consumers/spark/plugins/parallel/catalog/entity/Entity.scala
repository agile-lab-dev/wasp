package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.entity

import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.entity.ParallelWriteEntityJsonProtocol.{writeExecutionPlanRequestBodyFormat, writeExecutionPlanResponseBodyFormat}

import java.net.URL

case class Entity(override val baseUrl: URL) extends ParallelWriteEntity {
  private def getWriteExecutionPlanEndpoint(): URL = new URL(baseUrl, "/writeExecutionPlan")

  def getWriteExecutionPlan(requestBody: WriteExecutionPlanRequestBody): WriteExecutionPlanResponseBody = {
    post(getWriteExecutionPlanEndpoint(), Some(requestBody), Map.empty)(writeExecutionPlanRequestBodyFormat, writeExecutionPlanResponseBodyFormat)
  }
}
