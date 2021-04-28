package it.agilelab.bigdata.microservicecatalog.entity

import it.agilelab.bigdata.microservicecatalog.MicroserviceClient

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
case class WriteExecutionPlanResponseBody(writeUri: String, writeType: String, temporaryCredentials: Map[String, TemporaryCredential])
case class WriteExecutionPlanRequestBody(source: String, applicationFilter: Option[String] = None, dataGovernanceFilter: Option[String] = None)
case class TemporaryCredential(accessKeyID: String, secretKey: String, sessionToken: String)
