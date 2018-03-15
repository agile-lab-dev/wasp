package it.agilelab.bigdata.wasp.core.messages

import it.agilelab.bigdata.wasp.core.WaspMessage

object PipegraphMessages {

  sealed trait PipegraphMessage extends WaspMessage

  final case class StartPipegraph(pipegraphName: String) extends PipegraphMessage

  final case class StartPipegraphSuccess(pipegraphName: String) extends PipegraphMessage

  final case class StartPipegraphFailure(pipegraphName: String, error: String) extends PipegraphMessage

}
