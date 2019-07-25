package it.agilelab.bigdata.wasp.spark.plugins.telemetry


case class TelemetryPluginJMXTelemetryConfigModel(query: String, metricGroupAttribute: String, sourceIdAttribute: String, metricGroupFallback: String = "unknown", sourceIdFallback: String = "unknown")

case class TelemetryPluginTopicConfigModel(topicName: String,
                                           partitions: Int,
                                           replica: Int,
                                           kafkaSettings: Seq[(String, String)],
                                           jmx: Seq[TelemetryPluginJMXTelemetryConfigModel])
