package it.agilelab.bigdata.wasp.core.logging

import java.time.Instant

import it.agilelab.bigdata.wasp.core.logging.LogLevel.LogLevel


/**
	* Helper trait for logging support backed by a WaspLogger instance.
	*
	* @author Nicolò Bidotti
	*/
trait Logging {
	protected val logger = WaspLogger(this.getClass)
}

object LogLevel extends Enumeration {
	type LogLevel = Value
	val Trace,Info,Debug,Warn,Error = Value
}

object Logging {
	case class LogEvent(level: LogLevel, timestamp: Instant, thread:String, loggerName: String, message: String,maybeCause: Option[String] = None, maybeStackTrace: Option[String] = None)
}
