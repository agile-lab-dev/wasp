package it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.utils

import java.util.UUID

import com.typesafe.config.{Config, ConfigException}
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.KeyWithCorrelation
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._

object ConfigUtils {

  /* Gets keys from config if they are specified, else gets them from `inputKeys` */
  def keysToDelete(inputKeys: => Seq[KeyWithCorrelation],
                   maybeConfig: Option[Config],
                   configKey: String,
                   correlationIdConfigKey: String): Seq[KeyWithCorrelation] = {
    maybeConfig
      .flatMap { config =>
        getOptionalStringSeq(config, configKey).map {
          _.map(key => KeyWithCorrelation(key, getCorrelationId(config, correlationIdConfigKey)))
        }
      }
      .getOrElse(inputKeys)
  }

  /* Gets keys from config if they are specified, else gets them from `inputKeysRDD` */
  def keysToDeleteRDD(inputKeysRDD: RDD[KeyWithCorrelation],
                      maybeConfig: Option[Config],
                      configKey: String,
                      correlationIdConfigKey: String): RDD[KeyWithCorrelation] = {
    maybeConfig
      .flatMap { config =>
        getOptionalStringSeq(config, configKey).map {
          _.map(key => KeyWithCorrelation(key, getCorrelationId(config, correlationIdConfigKey)))
        }
      }.map(inputKeysRDD.sparkContext.parallelize(_))
      .getOrElse(inputKeysRDD)
  }

  private def getCorrelationId(config: Config, correlationIdKey: String) = {
    getOptionalString(config, correlationIdKey)
      .getOrElse(UUID.randomUUID().toString)
  }

  def getOptionalString(config: Config, key: String): Option[String] = {
    try {
      Option(config.getString(key))
    } catch {
      case _: ConfigException.Missing => None
    }
  }

  def getOptionalInt(config: Config, key: String): Option[Int] = {
    try {
      Option(config.getInt(key))
    } catch {
      case _: ConfigException.Missing => None
    }
  }
  def getOptionalBoolean(config: Config, key: String): Option[Boolean] = {
    try {
      Option(config.getBoolean(key))
    } catch {
      case _: ConfigException.Missing => None
    }
  }

  def getOptionalConfig(config: Config, key: String): Option[Config] = {
    try {
      Option(config.getConfig(key))
    } catch {
      case _: ConfigException.Missing => None
    }
  }

  def getOptionalStringSeq(config: Config, key: String): Option[Seq[String]] = {
    try {
      Option(config.getStringList(key)).map(_.asScala.toSeq)
    } catch {
      case _: ConfigException.Missing => None
    }
  }

}
