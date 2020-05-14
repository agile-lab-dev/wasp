package it.agilelab.bigdata.wasp.spark.plugins.telemetry

import org.apache.spark.SparkEnv

import scala.util.parsing.json.JSONObject
import org.apache.spark.ExecutorPlugin

class TelemetryPlugin extends SchedulingSupport with ConfigurationSupport with ExecutorPlugin {


  schedule(configuration.interval) { now =>

    val tag = SparkEnv.get.executorId


    val res = configuration.producer.telemetry.jmx.map {
      case TelemetryPluginJMXTelemetryConfigModel(query, metricGroupAttribute, sourceIdAttribute, metricGroupFallback, sourceIdFallback) =>
        JmxTelemetry.scrape(query = query,
                            tag = tag,
                            now = now,
                            metricGroupAttribute = metricGroupAttribute,
                            sourceIdAttribute = sourceIdAttribute,
                            metricGroupFallback = metricGroupFallback,
                            sourceIdFallback = sourceIdFallback)
    }.reduce(_ ++ _)


    res.map(x => (x("messageId").asInstanceOf[String], JSONObject(x).toString())).foreach {
      case (key, value) => TelemetryPluginProducer.send(configuration.producer, key, value)
    }


  }


}
