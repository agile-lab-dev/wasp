package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl

import org.apache.spark.sql.streaming.StreamingQuery

import scala.util.{Failure, Try}

/**
  * Trait collecting operations to be composed to realize Stopping of a [[it.agilelab.bigdata.wasp.models
  * .StructuredStreamingETLModel]]
  */
trait StoppingStep {

  /**
    * Stops the streaming query
    * @param query The streaming query to stop
    * @return The result of the stop action
    */
  protected def stop(query: StreamingQuery) = Try(query.stop()).recoverWith {
    case e: Throwable => Failure(new Exception(s"Cannot stop query ${query.name}", e))
  }

}
