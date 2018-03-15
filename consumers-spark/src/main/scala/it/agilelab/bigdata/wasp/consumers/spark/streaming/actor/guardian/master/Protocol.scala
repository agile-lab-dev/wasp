package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.guardian.master

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

  case object Initialize extends Protocol

  case object WorkAvailable extends Protocol

  case object GimmeWork extends Protocol

  case class WorkGiven(model: PipegraphModel, instance: PipegraphInstanceModel) extends Protocol

  case class WorkNotGiven(reason: Throwable) extends Protocol

  case class WorkNotCancelled(reason: Throwable) extends Protocol

  case class WorkFailed(reason: Throwable) extends Protocol

  case object CancelWork extends Protocol

  case object WorkCancelled extends Protocol

  case class RetryEnvelope[O](original:O, sender:ActorRef) extends Protocol

}