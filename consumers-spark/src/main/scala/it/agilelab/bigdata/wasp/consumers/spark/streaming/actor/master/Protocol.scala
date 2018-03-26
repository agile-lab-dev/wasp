package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master

import akka.actor.ActorRef
import it.agilelab.bigdata.wasp.core.models.{PipegraphInstanceModel, PipegraphModel}
import it.agilelab.bigdata.wasp.core.messages.PipegraphMessages

sealed trait Protocol


object Protocol {

  val StartPipegraph: PipegraphMessages.StartPipegraph.type = PipegraphMessages.StartPipegraph
  val PipegraphStarted: PipegraphMessages.PipegraphStarted.type = PipegraphMessages.PipegraphStarted
  val PipegraphNotStarted: PipegraphMessages.PipegraphNotStarted.type = PipegraphMessages.PipegraphNotStarted

  val StopPipegraph: PipegraphMessages.StopPipegraph.type = PipegraphMessages.StopPipegraph
  val PipegraphStopped: PipegraphMessages.PipegraphStopped.type = PipegraphMessages.PipegraphStopped
  val PipegraphNotStopped: PipegraphMessages.PipegraphNotStopped.type = PipegraphMessages.PipegraphNotStopped


  type StartPipegraph = PipegraphMessages.StartPipegraph
  type PipegraphStarted = PipegraphMessages.PipegraphStarted
  type PipegraphNotStarted = PipegraphMessages.PipegraphNotStarted

  type StopPipegraph = PipegraphMessages.StopPipegraph
  type PipegraphStopped = PipegraphMessages.PipegraphStopped
  type PipegraphNotStopped = PipegraphMessages.PipegraphNotStopped

  private[master] case object Initialize extends Protocol

  private[actor] case object WorkAvailable extends Protocol

  private[actor] case class WorkGiven(model: PipegraphModel, instance: PipegraphInstanceModel) extends Protocol

  private[actor] case class WorkNotGiven(reason: Throwable) extends Protocol

  private[actor] case class WorkFailed(reason: Throwable) extends Protocol

  private[actor] case object WorkCompleted extends Protocol

}