package it.agilelab.bigdata.wasp.core.logging

import it.agilelab.bigdata.wasp.core.WaspSystem
import org.slf4j.spi.LocationAwareLogger._
import org.slf4j.{Logger, LoggerFactory}


/**
  * SLF4J logger wrapper that also logs to the logger actor at `WaspSystem.loggerActor`
  */
final class WaspLogger(protected val slf4jLogger: Logger) {
  val loggerName: String = slf4jLogger.getName
  
  def error(msg: => String) {
    if (slf4jLogger.isErrorEnabled) {
      slf4jLogger.error(msg)
      remoteLog(ERROR_INT, msg)
    }
  }
  
  def error(msg: => String, t: Throwable) {
    if (slf4jLogger.isErrorEnabled) {
      slf4jLogger.error(msg, t)
      remoteLog(ERROR_INT, msg, t)
    }
  }
  
  def warn(msg: => String) {
    if (slf4jLogger.isWarnEnabled) {
      slf4jLogger.warn(msg)
      remoteLog(WARN_INT, msg)
    }
  }
  
  def warn(msg: => String, t: Throwable) {
    if (slf4jLogger.isWarnEnabled) {
      slf4jLogger.warn(msg, t)
      remoteLog(WARN_INT, msg, t)
    }
  }
  
  def info(msg: => String) {
    if (slf4jLogger.isInfoEnabled) {
      slf4jLogger.info(msg)
      remoteLog(INFO_INT, msg)
    }
  }
  
  def info(msg: => String, t: Throwable) {
    if (slf4jLogger.isInfoEnabled) {
      slf4jLogger.info(msg, t)
      remoteLog(INFO_INT, msg, t)
    }
  }
  
  def debug(msg: => String) {
    if (slf4jLogger.isDebugEnabled) {
      slf4jLogger.debug(msg)
      remoteLog(DEBUG_INT, msg)
    }
  }
  
  def debug(msg: => String, t: Throwable) {
    if (slf4jLogger.isDebugEnabled) {
      slf4jLogger.debug(msg, t)
      remoteLog(DEBUG_INT, msg, t)
    }
  }
  
  def trace(msg: => String) {
    if (slf4jLogger.isTraceEnabled) {
      slf4jLogger.trace(msg)
      remoteLog(TRACE_INT, msg)
    }
  }
  
  def trace(msg: => String, t: Throwable) {
    if (slf4jLogger.isTraceEnabled) {
      slf4jLogger.trace(msg, t)
      remoteLog(TRACE_INT, msg, t)
    }
  }
  
  protected def remoteLog(level: Int, msg: String, throwable: Throwable = null) {
    level match {
      case DEBUG_INT => WaspSystem.loggerActor ! akka.event.Logging.Debug(loggerName, classOf[WaspLogger], msg)
      case INFO_INT => WaspSystem.loggerActor ! akka.event.Logging.Info(loggerName, classOf[WaspLogger], msg)
      case WARN_INT => WaspSystem.loggerActor ! akka.event.Logging.Warning(loggerName, classOf[WaspLogger], msg)
      case ERROR_INT => WaspSystem.loggerActor ! akka.event.Logging.Error(loggerName, classOf[WaspLogger], msg)
    }
  }
}

object WaspLogger {
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