package it.agilelab.bigdata.wasp.core.logging

import java.time.Instant

import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.logging.LogLevel.LogLevel
import it.agilelab.bigdata.wasp.core.logging.Logging.LogEvent
import org.apache.commons.lang3.exception.ExceptionUtils
import org.slf4j.{Logger, LoggerFactory}

/**
  * SLF4J logger wrapper that also logs to the logger actor at `WaspSystem.loggerActor`
  */
private[logging] final class WaspLogger(protected val slf4jLogger: Logger) extends Serializable {
  val loggerName: String = slf4jLogger.getName
  
  def error(msg: => String) {
    if (slf4jLogger.isErrorEnabled) {
      slf4jLogger.error(msg)
      remoteLog(LogLevel.Error, msg)
    }
  }

  def error(msg: => String, t: Throwable) {
    if (slf4jLogger.isErrorEnabled) {
      slf4jLogger.error(msg, t)
      remoteLog(LogLevel.Error, msg, Option(t))
    }
  }

  def warn(msg: => String) {
    if (slf4jLogger.isWarnEnabled) {
      slf4jLogger.warn(msg)
      remoteLog(LogLevel.Warn, msg)
    }
  }

  def warn(msg: => String, t: Throwable) {
    if (slf4jLogger.isWarnEnabled) {
      slf4jLogger.warn(msg, t)
      remoteLog(LogLevel.Warn, msg, Option(t))
    }
  }

  def info(msg: => String) {
    if (slf4jLogger.isInfoEnabled) {
      slf4jLogger.info(msg)
      remoteLog(LogLevel.Info, msg)
    }
  }

  def info(msg: => String, t: Throwable) {
    if (slf4jLogger.isInfoEnabled) {
      slf4jLogger.info(msg, t)
      remoteLog(LogLevel.Info, msg, Option(t))
    }
  }

  def debug(msg: => String) {
    if (slf4jLogger.isDebugEnabled) {
      slf4jLogger.debug(msg)
      remoteLog(LogLevel.Debug, msg)
    }
  }

  def debug(msg: => String, t: Throwable) {
    if (slf4jLogger.isDebugEnabled) {
      slf4jLogger.debug(msg, t)
      remoteLog(LogLevel.Debug, msg, Option(t))
    }
  }

  def trace(msg: => String) {
    if (slf4jLogger.isTraceEnabled) {
      slf4jLogger.trace(msg)
      remoteLog(LogLevel.Trace, msg)
    }
  }

  def trace(msg: => String, t: Throwable) {
    if (slf4jLogger.isTraceEnabled) {
      slf4jLogger.trace(msg, t)
      remoteLog(LogLevel.Trace, msg, Option(t))
    }
  }

  def remoteLog(logLevel: LogLevel, msg:String, maybeThrowable: Option[Throwable] = None): Unit = {
    val maybeCause = maybeThrowable.map(_.getMessage)
    val maybeStackTrace = maybeThrowable.map(ExceptionUtils.getStackTrace)
    remoteLog(logLevel,msg,maybeCause,maybeStackTrace)
  }

  def remoteLog(logLevel: LogLevel, msg:String, maybeCause: Option[String], maybeStackTrace: Option[String]): Unit =
    remoteLog(LogEvent(logLevel, Instant.now(), Thread.currentThread().getName, loggerName, msg, maybeCause, maybeStackTrace))

  def remoteLog(event: LogEvent): Unit = if(WaspSystem.loggerActor != null) {
    WaspSystem.loggerActor ! event
  }
}

private[logging] object WaspLogger {
  /**
    * Creates a Logger named corresponding to the given class.
    * @param clazz Class used for the Logger's name. Must not be null!
    */
  def apply(clazz: Class[_]): WaspLogger = {
    require(clazz != null, "clazz must not be null!")
    logger(LoggerFactory getLogger clazz)
  }
  
  /**
    * Creates a Logger with the given name.
    * @param name The Logger's name. Must not be null!
    */
  def apply(name: String): WaspLogger = {
    require(name != null, "loggerName must not be null!")
    logger(LoggerFactory getLogger name)
  }
  
  private def logger(slf4jLogger: Logger): WaspLogger = slf4jLogger match {
    case _ => new WaspLogger(slf4jLogger)
  }
}