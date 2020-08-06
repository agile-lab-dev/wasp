package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph

import akka.actor.ActorRef
import akka.cluster.UniqueAddress
import it.agilelab.bigdata.wasp.models.StructuredStreamingETLModel

trait Protocol

object Protocol {

  /**
    * message sent from [[PipegraphGuardian]] to [[it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master.SparkConsumersStreamingMasterGuardian]]
    * to obtain work
    */
  private[actor] case class GimmeWork(member: UniqueAddress, pipegraph: String) extends Protocol


  /**
    * message sent from [[it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master.SparkConsumersStreamingMasterGuardian]]
    * to [[PipegraphGuardian]] to cancel current work
    */
  private[actor] case object CancelWork extends Protocol

  /**
    * message sent from [[PipegraphGuardian]] to [[it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master.SparkConsumersStreamingMasterGuardian]]
    * to signal successful cancellation
    */
  private[actor] case object WorkCancelled extends Protocol

  /**
    * message sent from [[PipegraphGuardian]] to [[it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master.SparkConsumersStreamingMasterGuardian]]
    * to signal unsuccessful cancellation
    *
    * @param reason The reason of failure
    */
  private[actor] case class WorkNotCancelled(reason: Throwable) extends Protocol


  /**
    * Self message to signal activation of etl
    *
    * @param etl the etl to activate
    */
  private[pipegraph] case class ActivateETL(etl: StructuredStreamingETLModel) extends Protocol

  /**
    * Self message to signal materialization of etl
    *
    * @param etl    the etl to materialize
    * @param worker the assigned worker
    */
  private[pipegraph] case class MaterializeETL(worker: ActorRef, etl: StructuredStreamingETLModel) extends Protocol

  /**
    * Self message to signal monitoring of etl
    *
    * @param etl    the etl to monitor
    * @param worker the assigned worker
    */
  private[pipegraph] case class MonitorETL(worker: ActorRef, etl: StructuredStreamingETLModel) extends
    Protocol

  /**
    * Self message to signal Finish of Activation
    */
  private[pipegraph] case object ActivationFinished extends Protocol

  /**
    * Self message to signal Start of Materialization
    */
  private[pipegraph] case object MaterializePipegraph extends Protocol

  /**
    * Self message to signal Finish of Materialization
    */
  private[pipegraph] case object MaterializationFinished extends Protocol

  /**
    * Self message to signal Start of Monitoring
    */
  private[pipegraph] case object MonitorPipegraph extends Protocol

  /**
    * Self message to signal Finish of Monitoring
    */
  private[pipegraph] case object MonitoringFinished extends Protocol

  /**
    * Self message to signal start of stopping of etl
    */
  private[pipegraph] case class StopETL(worker: ActorRef, etl: StructuredStreamingETLModel) extends Protocol

  /**
    * Self message to signal Finish of Stop
    */
  private[pipegraph] case object StopFinished extends Protocol

  /**
    * Self message to shutdown the [[PipegraphGuardian]]
    */
  private[pipegraph] case object Shutdown extends Protocol


  /**
    * Self message to signal need to perform a retry in current state
    */
  private[pipegraph] case object PerformRetry extends Protocol


}
