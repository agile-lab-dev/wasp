package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl

import org.apache.spark.sql.streaming.StreamingQuery

import scala.util.{Failure, Try}

trait StoppingStep {

  def stop(query: StreamingQuery) = Try(query.stop()).recoverWith {
    case e: Throwable => Failure(new Exception(s"Cannot stop query ${query.name}", e))
  }

}
