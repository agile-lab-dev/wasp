package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master

import it.agilelab.bigdata.wasp.core.messages.PipegraphMessages
import it.agilelab.bigdata.wasp.core.messages
import it.agilelab.bigdata.wasp.models.{PipegraphInstanceModel, PipegraphModel}

/**
  * Trait marking messages as being part of the [[SparkConsumersStreamingMasterGuardian]] protocol.
  */
sealed trait Protocol


object Protocol {

  /**
    * Alias the type of [[PipegraphMessages.StartPipegraph]]
    */
  type StartPipegraph = PipegraphMessages.StartPipegraph
  /**
    * Alias the type of [[PipegraphMessages.PipegraphStarted]]
    */
  type PipegraphStarted = PipegraphMessages.PipegraphStarted
  /**
    * Alias the type of [[PipegraphMessages.PipegraphNotStarted]]
    */
  type PipegraphNotStarted = PipegraphMessages.PipegraphNotStarted
  /**
    * Alias the type of [[PipegraphMessages.StopPipegraph]]
    */
  type StopPipegraph = PipegraphMessages.StopPipegraph
  /**
    * Alias the type of [[PipegraphMessages.PipegraphStopped]]
    */
  type PipegraphStopped = PipegraphMessages.PipegraphStopped
  /**
    * Alias the type of [[PipegraphMessages.PipegraphNotStopped]]
    */
  type PipegraphNotStopped = PipegraphMessages.PipegraphNotStopped
  /**
    * Alias the companion object of [[PipegraphMessages.StartPipegraph]]
    */
  val StartPipegraph: PipegraphMessages.StartPipegraph.type = PipegraphMessages.StartPipegraph
  /**
    * Alias the companion object of [[PipegraphMessages.PipegraphStarted]]
    */
  val PipegraphStarted: PipegraphMessages.PipegraphStarted.type = PipegraphMessages.PipegraphStarted
  /**
    * Alias the companion object of [[PipegraphMessages.PipegraphNotStarted]]
    */
  val PipegraphNotStarted: PipegraphMessages.PipegraphNotStarted.type = PipegraphMessages.PipegraphNotStarted
  /**
    * Alias the companion object of [[PipegraphMessages.StopPipegraph]]
    */
  val StopPipegraph: PipegraphMessages.StopPipegraph.type = PipegraphMessages.StopPipegraph
  /**
    * Alias the companion object of [[PipegraphMessages.PipegraphStopped]]
    */
  val PipegraphStopped: PipegraphMessages.PipegraphStopped.type = PipegraphMessages.PipegraphStopped
  /**
    * Alias the companion object of [[PipegraphMessages.PipegraphNotStopped]]
    */
  val PipegraphNotStopped: PipegraphMessages.PipegraphNotStopped.type = PipegraphMessages.PipegraphNotStopped


  /**
    * Alias object  [[messages.RestartConsumers]]
    */
  val RestartConsumers: messages.RestartConsumers.type = messages.RestartConsumers



  /**
    * Alias object  [[PipegraphMessages.StartSystemPipegraphs]]
    */
  val StartSystemPipegraphs: PipegraphMessages.StartSystemPipegraphs.type = PipegraphMessages.StartSystemPipegraphs

  /**
    * Alias object  [[PipegraphMessages.SystemPipegraphsStarted]]
    */
  val SystemPipegraphsStarted: PipegraphMessages.SystemPipegraphsStarted.type = PipegraphMessages.SystemPipegraphsStarted

  /**
    * Message sent from [[SparkConsumersStreamingMasterGuardian]] to
    * [[it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph.PipegraphGuardian]]
    * to signal that work has been given to it
    *
    * @param model    The given work
    * @param instance The instance currently representing the [[PipegraphModel]] to start
    */
  private[actor] case class WorkGiven(model: PipegraphModel, instance: PipegraphInstanceModel) extends Protocol

  /**
    * Message sent from [[SparkConsumersStreamingMasterGuardian]] to
    * [[it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph.PipegraphGuardian]]
    * to signal that work has not been given to it due to an error
    *
    */
  private[actor] case class WorkNotGiven(reason: Throwable) extends Protocol

  private[actor] case class WorkFailed(reason: Throwable) extends Protocol

  /**
    * Self message sent by [[SparkConsumersStreamingMasterGuardian]] to itself to signal that a restart happened.
    */
  private[master] case object ConsumersRestarted




  /**
    * Message sent from [[SparkConsumersStreamingMasterGuardian]] to itself
    */
  private[master] case object Initialize extends Protocol

  /**
    * Message sent from [[SparkConsumersStreamingMasterGuardian]] to
    * [[it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph.PipegraphGuardian]]
    * to signal that work is available
    */
  private[actor] case class WorkAvailable(name: String) extends Protocol

  /**
    * Message sent from [[it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph.PipegraphGuardian]] to
    * [[SparkConsumersStreamingMasterGuardian]] to signal that Work completed naturally and Successfully
    */
  private[actor] case object WorkCompleted extends Protocol

}