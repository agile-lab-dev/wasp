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
  def getWriteExecutionPlan(): WriteExecutionPlanResponseBody
}

// Parallel write
object ParallelWriteFormat extends Enumeration {
  type ParallelWriteFormat = Value
  val parquet = Value("Parquet")
  val delta = Value("Delta")
}
case class WriteExecutionPlanResponseBody(format: String, writeUri: String, writeType: String, temporaryCredentials: TemporaryCredentials)
case class WriteExecutionPlanRequestBody(source: String, applicationFilter: Option[String] = None, dataGovernanceFilter: Option[String] = None)
case class TemporaryCredentials(r: TemporaryCredential, w: TemporaryCredential)
case class TemporaryCredential(accessKeyID: String, secretKey: String, sessionToken: String)
