package it.agilelab.bigdata.wasp.spark.plugins.telemetry

import java.lang.management.ManagementFactory
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.UUID

import javax.management.ObjectName

import scala.annotation.tailrec
import scala.util.Try
import scala.collection.JavaConverters._

object JmxTelemetry {

  val camelCaseToKebabCaseNormalizer: String => String = { str =>
    @tailrec
    def camel2SnakeRec(s: String, output: String, lastUppercase: Boolean): String =
      if (s.isEmpty) output
      else {
        val c = if (s.head.isUpper && !lastUppercase) "-" + s.head.toLower else s.head.toLower
        camel2SnakeRec(s.tail, output + c, s.head.isUpper && !lastUppercase)
      }

    if (str.forall(_.isUpper)) str.map(_.toLower)
    else {
      camel2SnakeRec(str, "", true)
    }
  }
  val spaceToDashesNormalizer: String => String = _.replaceAll(" ", "-")
  val lowercaseIfDashesNormalizer: String => String = str => if (str.contains("-")) {
    str.toLowerCase()
  } else {
    str
  }

  val removeBraces: String => String = _.replaceAll("\\(", "").replaceAll("\\)", "")

  val removeQuotes: String => String = _.replaceAll("\"", "")

  val underscoreNormalizer: String => String = _.replaceAll("_", "")

  val defaultNormalizer: String => String = Seq(removeBraces,
                                                removeQuotes,
                                                underscoreNormalizer,
                                                spaceToDashesNormalizer,
                                                lowercaseIfDashesNormalizer,
                                                camelCaseToKebabCaseNormalizer).reduce(_.andThen(_))

  def scrape(query: String,
             tag: String,
             now: Instant,
             metricGroupAttribute: String,
             sourceIdAttribute: String,
             metricGroupFallback: String = "unknown",
             sourceIdFallback: String = "unknown",
             normalizer: String => String = defaultNormalizer): Seq[Map[String, Any]] = {
    val mbeanServer = ManagementFactory.getPlatformMBeanServer

    val messageId = UUID.randomUUID().toString
    val timestamp = DateTimeFormatter.ISO_INSTANT.format(now)



    for {
      mbeanName <- mbeanServer.queryNames(ObjectName.getInstance(query), null).asScala.toVector
      attribute <- mbeanServer.getMBeanInfo(mbeanName).getAttributes
      attributeName = attribute.getName
      value <- Try(mbeanServer.getAttribute(mbeanName, attributeName)).toOption.filter(isSupportedType)
    } yield {

      val prefix = removeQuotes(mbeanName.getDomain)
      val sourceId = Option(mbeanName.getKeyProperty(sourceIdAttribute)).map(normalizer).getOrElse(sourceIdFallback)
      val metricName = normalizer(attribute.getName)
      val metricGroup = Option(mbeanName.getKeyProperty(metricGroupAttribute)).map(normalizer).getOrElse(metricGroupFallback)
      val completeName = s"$prefix.$metricGroup.$metricName"

      val header = Map("messageId" -> messageId,
        "sourceId" -> sourceId,
        "timestamp" -> timestamp,
        "tag" -> tag)

      metric(header, completeName, value)
    }
  }

  def isSupportedType(obj: Any): Boolean = obj.isInstanceOf[Long] ||
    obj.isInstanceOf[Int] ||
    obj.isInstanceOf[Short] ||
    obj.isInstanceOf[Byte] ||
    obj.isInstanceOf[Double] ||
    obj.isInstanceOf[Float]

  def metric(header: Map[String, Any], metric: String, value: Any): Map[String, Any] =
    header + ("metric" -> metric) + ("value" -> value)



}
