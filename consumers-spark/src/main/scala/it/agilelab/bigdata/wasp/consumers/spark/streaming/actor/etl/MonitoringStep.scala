package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl

import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryException, StreamingQueryProgress, StreamingQueryStatus}

import scala.util.{Failure, Try}

/**
  * Trait collecting operations to be composed to realize Monitoring of a [[it.agilelab.bigdata.wasp.core.models.StructuredStreamingETLModel]]
  */
trait MonitoringStep {

  case class MonitorOutcome(isActive: Boolean,
                            status: StreamingQueryStatus,
                            progress: StreamingQueryProgress,
                            option: Option[StreamingQueryException])


  /**
    * Monitors a streaming query.
    * @param query The query to be monitored
    * @return The [[MonitorOutcome]]
    */
  protected def monitor(query: StreamingQuery) = Try(MonitorOutcome(query.isActive, query.status, query.lastProgress, query
    .exception)).recoverWith {
    case e: Throwable => Failure(new Exception(s"Failed monitoring of query ${query.name}", e))
  }
}
