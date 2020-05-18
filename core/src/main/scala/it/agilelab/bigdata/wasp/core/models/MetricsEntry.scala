package it.agilelab.bigdata.wasp.core.models

import java.time.Instant

case class MetricEntry(source: SourceEntry, name: String)

case class SourceEntry(name: String)

case class TelemetryEntry(source: SourceEntry,
                          metric: MetricEntry,
                          messageId: String,
                          value: Long,
                          timestamp: Instant)

case class Metrics(found: Long, entries: Seq[MetricEntry])

case class Sources(found: Long, entries: Seq[SourceEntry])

case class TelemetrySeries(source: SourceEntry, metric: MetricEntry, series: Seq[TelemetryPoint])


case class TelemetryPoint(timestamp: Instant, value: Double)

object Aggregate extends Enumeration {
  type Aggregate = Value
  val Max: Aggregate = Value("max")
  val Min: Aggregate = Value("min")
  val Avg: Aggregate = Value("avg")
  val Sum: Aggregate = Value("sum")
}