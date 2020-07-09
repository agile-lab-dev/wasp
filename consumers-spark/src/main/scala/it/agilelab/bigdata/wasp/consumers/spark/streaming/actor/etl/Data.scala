package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery

/**
  * Trait marking classes holding [[StructuredStreamingETLActor]] State Data
  */
sealed trait Data

object Data {

  /**
    * No Data
    */
  case object IdleData extends Data

  /**
    * Data held by the state machine when in [[State.WaitingToBeMaterialized]]
    * @param dataFrame The activated [[DataFrame]]
    */
  case class ActivatedData(dataFrame: DataFrame) extends Data

  /**
    * Data held by the state machine when in [[State.WaitingToBeMonitored]]
    * @param streamingQuery The streaming query to be monitored
    */
  case class MaterializedData(streamingQuery: StreamingQuery) extends Data
}