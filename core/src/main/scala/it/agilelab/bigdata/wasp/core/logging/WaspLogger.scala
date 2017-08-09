package it.agilelab.bigdata.wasp.core.logging

import akka.event.slf4j.Logger
import it.agilelab.bigdata.wasp.core.WaspSystem
import org.slf4j.spi.LocationAwareLogger._
import org.slf4j.{Logger, LoggerFactory}

object WaspLogger {

  /*
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
    case _ =>
      new WaspDefaultLogger(slf4jLogger)
  }
}

/**
 * wrapper di slf4j
 * In questo punto possiamo andare a espandere le funzionalita, per esempio loggando anche su un actor remoto
 */
trait WaspLogger extends {


  protected val slf4jLogger: Logger = LoggerFactory.getLogger(getClass)
  val loggerName = slf4jLogger.getName


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

  /*def updateReference(test : Option[ActorRef]){
    WaspLogger.loggerActor = test
  }*/

  protected final def remoteLog(level: Int, msg: String, throwable: Throwable = null) {
    if (WaspSystem.loggerActor.isDefined) {
      level match {
        case DEBUG_INT => WaspSystem.loggerActor.get ! akka.event.Logging.Debug(loggerName, classOf[WaspDefaultLogger], msg)
        case INFO_INT => WaspSystem.loggerActor.get ! akka.event.Logging.Info(loggerName, classOf[WaspDefaultLogger], msg)
        case WARN_INT => WaspSystem.loggerActor.get ! akka.event.Logging.Warning(loggerName, classOf[WaspDefaultLogger], msg)
        case ERROR_INT => WaspSystem.loggerActor.get ! akka.event.Logging.Error(loggerName, classOf[WaspDefaultLogger], msg)

      }
    } else {
      println("WaspLogger instance is missing!")
    }
  }

}

final class WaspDefaultLogger(override protected val slf4jLogger: Logger) extends WaspLogger


