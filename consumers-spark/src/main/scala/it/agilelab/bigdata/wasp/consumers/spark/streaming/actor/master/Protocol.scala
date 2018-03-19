package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master

import akka.actor.ActorRef
import it.agilelab.bigdata.wasp.core.models.{PipegraphInstanceModel, PipegraphModel}

sealed trait Protocol


object Protocol {

  case class StartPipegraph(name: String) extends Protocol

  case class PipegraphStarted(name: String) extends Protocol

  case class PipegraphNotStarted(name: String, reason: String) extends Protocol

  case class StopPipegraph(name: String) extends Protocol

  case class PipegraphStopped(name: String) extends Protocol

  case class PipegraphNotStopped(name: String, reason: String) extends Protocol

  private[master] case object Initialize extends Protocol

  private[actor] case object WorkAvailable extends Protocol

  private[actor] case class WorkGiven(model: PipegraphModel, instance: PipegraphInstanceModel) extends Protocol

  private[actor] case class WorkNotGiven(reason: Throwable) extends Protocol

  case class WorkFailed(reason: Throwable) extends Protocol

}