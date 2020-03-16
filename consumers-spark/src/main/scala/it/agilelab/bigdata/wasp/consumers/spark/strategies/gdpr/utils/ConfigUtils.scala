package it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.utils

import com.typesafe.config.{Config, ConfigException}
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._

object ConfigUtils {

  /* Gets keys from config if they are specified, else gets them from `inputKeys` */
  def keysToDelete(inputKeys: => Seq[String], maybeConfig: Option[Config], configKey: String): Seq[String] = {
    maybeConfig
      .flatMap(getOptionalStringSeq(_, configKey))
      .getOrElse(inputKeys)
  }

  /* Gets keys from config if they are specified, else gets them from `inputKeysRDD` */
  def keysToDeleteRDD(inputKeysRDD: RDD[String], maybeConfig: Option[Config], configKey: String): RDD[String] = {
    maybeConfig
      .flatMap(getOptionalStringSeq(_, configKey))
      .map(inputKeysRDD.sparkContext.parallelize(_))
      .getOrElse(inputKeysRDD)
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
