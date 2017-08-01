package it.agilelab.bigdata.wasp.core.logging

import it.agilelab.bigdata.wasp.core.WaspSystem
import akka.actor.Props

/**
 *
 */
trait LoggerInjector {
  /**
   * Logger actor's Akka Props.
   * Subclasses are forced to give define a proper return value
   */
  def loggerActorProps: Props

  /**
   * Logger actor's name.
   * Initialized with a normalized class name by default
   */
  def loggerActorName: String = this.getClass.getSimpleName.replace("$", "")

  /**
   * Logger actor.
   * Initialized with <code>loggerActorProps</code> and <code>loggerActorName</code>
   */
  WaspSystem.initializeLoggerActor(loggerActorProps, loggerActorName)
}