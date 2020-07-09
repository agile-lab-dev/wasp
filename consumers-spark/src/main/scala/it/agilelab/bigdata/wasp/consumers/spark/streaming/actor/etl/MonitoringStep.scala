package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl

import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryException, StreamingQueryProgress, StreamingQueryStatus}

import scala.util.{Failure, Try}

/**
  * Trait collecting operations to be composed to realize Monitoring of a [[it.agilelab.bigdata.wasp.models.StructuredStreamingETLModel]]
  */
trait MonitoringStep {




  /**
    * Monitors a streaming query.
    * @param query The query to be monitored
    * @return The [[MonitorOutcome]]
    */
  protected def monitor(query: StreamingQuery) = Try(MonitorOutcome(query.isActive, query.status, Option(query
    .lastProgress), query
    .exception)).recoverWith {
    case e: Throwable => Failure(new Exception(s"Failed monitoring of query ${query.name}", e))
  }
}
