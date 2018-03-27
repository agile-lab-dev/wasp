package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl

import it.agilelab.bigdata.wasp.core.models.StructuredStreamingETLModel

/**
  * Trait marking messages as being part of the [[StructuredStreamingETLActor]] protocol.
  */
sealed trait Protocol

object Protocol {

  /**
    * Message sent from [[it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph.PipegraphGuardian]] to
    * [[StructuredStreamingETLActor]] to request activation of a [[StructuredStreamingETLModel]]
    * @param etl the etl to activate
    */
  private[actor] case class ActivateETL(etl: StructuredStreamingETLModel) extends Protocol

  /**
    * Message sent from [[StructuredStreamingETLActor]] to [[it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph.PipegraphGuardian]]
    * to signal successful activation of a [[StructuredStreamingETLModel]]
    * @param etl The etl activated
    */
  private[actor] case class ETLActivated(etl: StructuredStreamingETLModel) extends Protocol

  /**
    * Message sent from [[StructuredStreamingETLActor]] to [[it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph.PipegraphGuardian]]
    * to signal failed activation of a [[StructuredStreamingETLModel]]
    * @param etl The etl whose activation failed
    * @param reason The reason of the activation failure
    */
  private[actor] case class ETLNotActivated(etl: StructuredStreamingETLModel, reason: Throwable) extends Protocol

  /**
    * Message sent from [[it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph.PipegraphGuardian]] to
    * [[StructuredStreamingETLActor]] to request materialization of a [[StructuredStreamingETLModel]]
    * @param etl the etl to materialize
    */
  private[actor] case class MaterializeETL(etl: StructuredStreamingETLModel) extends Protocol


  /**
    * Message sent from [[StructuredStreamingETLActor]] to [[it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph.PipegraphGuardian]]
    * to signal successful materialization of a [[StructuredStreamingETLModel]]
    * @param etl The etl materialized
    */
  private[actor] case class ETLMaterialized(etl: StructuredStreamingETLModel) extends Protocol

  /**
    * Message sent from [[StructuredStreamingETLActor]] to [[it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph.PipegraphGuardian]]
    * to signal failed materialization of a [[StructuredStreamingETLModel]]
    * @param etl The etl whose materialization failed
    * @param reason The reason of the materialization failure
    */
  private[actor] case class ETLNotMaterialized(etl: StructuredStreamingETLModel, reason:Throwable) extends Protocol

  /**
    * Message sent from [[it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph.PipegraphGuardian]] to
    * [[StructuredStreamingETLActor]] to request monitoring of a [[StructuredStreamingETLModel]]
    * @param etl the etl to monitor
    */
  private[actor] case class CheckETL(etl: StructuredStreamingETLModel) extends Protocol


  /**
    * Message sent from [[StructuredStreamingETLActor]] to [[it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph.PipegraphGuardian]]
    * to signal failed monitoring of a [[StructuredStreamingETLModel]]
    * @param etl The etl whose monitoring failed
    * @param reason The reason of the monitoring failure
    */
  private[actor] case class ETLCheckFailed(etl:StructuredStreamingETLModel, reason:Throwable) extends Protocol

  /**
    * Message sent from [[StructuredStreamingETLActor]] to [[it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph.PipegraphGuardian]]
    * to signal successful monitoring of a [[StructuredStreamingETLModel]]
    * @param etl The etl monitored
    */
  private[actor] case class ETLCheckSucceeded(etl:StructuredStreamingETLModel) extends Protocol


  /**
    * Message sent from [[it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph.PipegraphGuardian]] to
    * [[StructuredStreamingETLActor]] to request stop of a [[StructuredStreamingETLModel]]
    * @param etl the etl to stop
    */
  private[actor] case class StopETL(etl:StructuredStreamingETLModel) extends Protocol


  /**
    * Message sent from [[StructuredStreamingETLActor]] to [[it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph.PipegraphGuardian]]
    * to signal successful stop of a [[StructuredStreamingETLModel]]
    * @param etl The etl stopped
    */
  private[actor] case class ETLStopped(etl:StructuredStreamingETLModel) extends Protocol

}
