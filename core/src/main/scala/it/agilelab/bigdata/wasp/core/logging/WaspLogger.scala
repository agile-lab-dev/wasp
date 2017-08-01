package it.agilelab.bigdata.wasp.core.logging

import org.slf4j.Logger
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.spi.LocationAwareLogger

import akka.actor.Actor
import akka.actor.ActorRef
import akka.event.Logging.Debug
import akka.event.Logging.Error
import akka.event.Logging.Info
import akka.event.Logging.InitializeLogger
import akka.event.Logging.LoggerInitialized
import akka.event.Logging.Warning
import akka.event.slf4j.Slf4jLogger
import it.agilelab.bigdata.wasp.core.WaspSystem

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
    case locationAwareLogger: LocationAwareLogger =>
      new WaspDefaultLocationAwareLogger(locationAwareLogger)
    case _ =>
      new WaspDefaultLogger(slf4jLogger)
  }
}

/**
 * wrapper di slf4j
 * In questo punto possiamo andare a espandere le funzionalita, per esempio loggando anche su un actor remoto
 */
trait WaspLogger extends {

  import LocationAwareLogger.{ERROR_INT, WARN_INT, INFO_INT, DEBUG_INT, TRACE_INT}


  val loggerName = slf4jLogger.getName

  protected val slf4jLogger: Logger


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


trait WaspLocationAwareLogger extends WaspLogger {

  import LocationAwareLogger.{ERROR_INT, WARN_INT, INFO_INT, DEBUG_INT, TRACE_INT}

  override protected val slf4jLogger: LocationAwareLogger


  protected val wrapperClassName: String

  override def error(msg: => String) {
    if (slf4jLogger.isErrorEnabled) log(ERROR_INT, msg)
  }

  override def error(msg: => String, t: Throwable) {
    if (slf4jLogger.isErrorEnabled) log(ERROR_INT, msg, t)
  }

  override def warn(msg: => String) {
    if (slf4jLogger.isWarnEnabled) log(WARN_INT, msg)
  }

  override def warn(msg: => String, t: Throwable) {
    if (slf4jLogger.isWarnEnabled) log(WARN_INT, msg, t)
  }

  override def info(msg: => String) {
    if (slf4jLogger.isInfoEnabled) log(INFO_INT, msg)
  }

  override def info(msg: => String, t: Throwable) {
    if (slf4jLogger.isInfoEnabled) log(INFO_INT, msg, t)
  }

  override def debug(msg: => String) {
    if (slf4jLogger.isDebugEnabled) log(DEBUG_INT, msg)
  }

  override def debug(msg: => String, t: Throwable) {
    if (slf4jLogger.isDebugEnabled) log(DEBUG_INT, msg, t)
  }

  override def trace(msg: => String) {
    if (slf4jLogger.isTraceEnabled) log(TRACE_INT, msg)
  }

  override def trace(msg: => String, t: Throwable) {
    if (slf4jLogger.isTraceEnabled) log(TRACE_INT, msg, t)
  }

  private final def log(level: Int, msg: String, throwable: Throwable = null) {
    slf4jLogger.log(null, wrapperClassName, level, msg, null, throwable)

    remoteLog(level, msg, throwable)
  }
}

object WaspDefaultLocationAwareLogger {
  private val WrapperClassName = classOf[WaspDefaultLocationAwareLogger].getName
}

final class WaspDefaultLocationAwareLogger(override protected val slf4jLogger: LocationAwareLogger)
  extends WaspLocationAwareLogger {
  override protected val wrapperClassName = WaspDefaultLocationAwareLogger.WrapperClassName
}

