package it.agilelab.bigdata.wasp.spark.plugins.telemetry

import scala.concurrent.duration.Duration

case class TelemetryPluginConfiguration(interval: Duration, producer: TelemetryMetadataProducerConfig)
