package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph

import akka.actor.ActorRef
import it.agilelab.bigdata.wasp.core.models.StructuredStreamingETLModel

trait Protocol

object Protocol {

  case object GimmeWork extends Protocol

  case object CancelWork extends Protocol

  case object WorkCancelled extends Protocol

  case class WorkNotCancelled(reason: Throwable) extends Protocol

  private[pipegraph] case class ActivateETL(etl: StructuredStreamingETLModel) extends Protocol

  private[pipegraph] case class MaterializeETL(worker: ActorRef, etl: StructuredStreamingETLModel) extends Protocol

  private[pipegraph] case class MonitorETL(worker:ActorRef, etl: StructuredStreamingETLModel) extends
    Protocol

  private[pipegraph] case object ActivationFinished extends Protocol

  private[pipegraph] case object MaterializePipegraph extends Protocol

  private[pipegraph] case object MaterializationFinished extends Protocol

  private[pipegraph] case object MonitorPipegraph extends Protocol

  private[pipegraph] case object MonitoringFinished extends Protocol

  private[pipegraph] case class StopETL(worker: ActorRef,etl:StructuredStreamingETLModel) extends Protocol

  private[pipegraph] case object StopFinished extends Protocol

  private[pipegraph] case object Shutdown extends Protocol


  private[pipegraph] case object PerformRetry extends Protocol



}
