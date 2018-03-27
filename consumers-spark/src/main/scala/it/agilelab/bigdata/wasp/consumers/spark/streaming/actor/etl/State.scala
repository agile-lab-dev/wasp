package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl

/**
  * Trait marking objects representing [[StructuredStreamingETLActor]] State
  */
sealed trait State

object State {

  /**
    * [[StructuredStreamingETLActor]] is waiting to be activated by the [[it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph.PipegraphGuardian]]
    */
  case object WaitingToBeActivated extends State

  /**
    * [[StructuredStreamingETLActor]] is waiting to be materialized by the [[it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph.PipegraphGuardian]]
    */
  case object WaitingToBeMaterialized extends State

  /**
    * [[StructuredStreamingETLActor]] is waiting to be monitored by the [[it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph.PipegraphGuardian]]
    */
  case object WaitingToBeMonitored extends State

}