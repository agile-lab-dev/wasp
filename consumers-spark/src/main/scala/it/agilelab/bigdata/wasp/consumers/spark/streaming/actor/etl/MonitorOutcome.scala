package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl

import org.apache.spark.sql.streaming.{StreamingQueryException, StreamingQueryProgress, StreamingQueryStatus}

case class MonitorOutcome(isActive: Boolean,
                          status: StreamingQueryStatus,
                          progress: Option[StreamingQueryProgress],
                          option: Option[StreamingQueryException])
