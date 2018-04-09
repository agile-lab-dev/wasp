package it.agilelab.bigdata.wasp.core.messages

import it.agilelab.bigdata.wasp.core.WaspMessage

object PipegraphMessages {

  sealed trait PipegraphMessage extends WaspMessage
  sealed trait StartPipegraphResult extends PipegraphMessage
  sealed trait StopPipegraphResult extends PipegraphMessage

  case object StartSystemPipegraphs extends  PipegraphMessage

  case object SystemPipegraphsStarted extends PipegraphMessage

  case class StartPipegraph(name: String) extends PipegraphMessage

  case class PipegraphStarted(name: String, instanceName: String) extends StartPipegraphResult

  case class PipegraphNotStarted(name: String, reason: String) extends StartPipegraphResult


  case class StopPipegraph(name: String) extends PipegraphMessage

  case class PipegraphStopped(name: String) extends StopPipegraphResult

  case class PipegraphNotStopped(name: String, reason: String) extends StopPipegraphResult
}
