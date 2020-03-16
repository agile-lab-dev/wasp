package it.agilelab.bigdata.wasp.core.logging

/**
  * Trait useful to log in spark. this traits correctly handles spark serializability of closures.
  */
trait GuardedLogging extends Serializable {

  protected def loggerName: String = this.getClass.getCanonicalName

  @transient protected lazy val log: WaspLogger = WaspLogger(this.getClass)

}

