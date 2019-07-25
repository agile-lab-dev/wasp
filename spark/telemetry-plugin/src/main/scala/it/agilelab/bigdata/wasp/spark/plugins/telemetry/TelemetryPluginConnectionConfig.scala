package it.agilelab.bigdata.wasp.spark.plugins.telemetry

case class TelemetryPluginConnectionConfig(protocol: String,
                                           host: String,
                                           port: Int = 0,
                                           timeout: Option[Long] = None,
                                           metadata: Option[Map[String, String]])
