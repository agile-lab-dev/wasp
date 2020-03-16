package it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.exception

import com.typesafe.config.ConfigException

object ConfigExceptions {

  case class RawDataConfigException(configException: ConfigException,
                                    message: String = "Missing mandatory configuration key") extends Exception(message, configException)

  case class KeyValueConfigException(throwable: Throwable, message: String) extends Exception(message, throwable)

}
