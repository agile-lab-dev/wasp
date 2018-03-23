package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl

import it.agilelab.bigdata.wasp.core.models.StructuredStreamingETLModel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery

sealed trait Data

object Data {

  case object IdleData extends Data
  case class ActivatedData(dataFrame: DataFrame) extends Data
  case class MaterializedData(streamingQuery: StreamingQuery) extends Data
}