package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl

import it.agilelab.bigdata.wasp.core.models.StructuredStreamingETLModel

sealed trait Protocol

object Protocol {

  case class WorkFailed(etl: StructuredStreamingETLModel, reason: Throwable)



  private[actor] case class ActivateETL(etl: StructuredStreamingETLModel)

  private[actor] case class ETLActivated(etl: StructuredStreamingETLModel) extends Protocol

  private[actor] case class ETLNotActivated(etl: StructuredStreamingETLModel, reason: Throwable) extends Protocol

  private[actor] case class MaterializeETL(etl: StructuredStreamingETLModel)

  private[actor] case class ETLMaterialized(etl: StructuredStreamingETLModel) extends Protocol

  private[actor] case class ETLNotMaterialized(etl: StructuredStreamingETLModel) extends Protocol


  private[actor] case class CheckETL(etl: StructuredStreamingETLModel) extends Protocol
  private[actor] case class ETLCheckFailed(etl:StructuredStreamingETLModel, reason:Throwable) extends Protocol
  private[actor] case class ETLCheckSucceeded(etl:StructuredStreamingETLModel) extends Protocol

  private[actor] case class StopETL(etl:StructuredStreamingETLModel) extends Protocol

  private[actor] case class ETLStopped(etl:StructuredStreamingETLModel) extends Protocol

}
