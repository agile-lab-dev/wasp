package it.agilelab.bigdata.wasp.master.web.controllers

import it.agilelab.bigdata.wasp.core.eventengine.eventproducers.SolrEventIndex
import it.agilelab.bigdata.wasp.core.{SolrLoggerIndex, SolrTelemetryIndexModel}
import it.agilelab.bigdata.wasp.models.{CountEntry, Counts}
import org.apache.solr.client.solrj.response.QueryResponse

import java.time.Instant
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

trait StatsService {
  def counts(startTimestamp: Instant, endTimestamp: Instant, limit: Int): Future[Counts]
}

class DefaultSolrStatsService(client: SolrClient)(
    implicit ec: ExecutionContext
) extends StatsService {

  private val TIMESTAMP_FIELD = "timestamp"

  private val GAP = "+1MINUTE"

  private def mapPivot(response: QueryResponse, fieldPivot: PivotField): Seq[CountEntry] = {

    val globalRange = response.getFacetRanges
      .get(0)
      .getCounts
      .asScala
      .map { countEntry =>
        val timestamp = Instant.parse(countEntry.getValue)
        val count     = countEntry.getCount
        (timestamp, count)
      }
      .toMap

    val x: Map[Instant, mutable.Buffer[(String, Instant, Int)]] = response.getFacetPivot
      .get(fieldPivot.field)
      .asScala
      .flatMap { fieldPivotEntry =>
        val range = fieldPivotEntry.getFacetRanges.asScala.head

        range.getCounts.asScala.map { entry =>
          val timestamp  = Instant.parse(entry.getValue)
          val count      = entry.getCount
          val facetValue = fieldPivotEntry.getValue.asInstanceOf[String]

          (facetValue, timestamp, count)
        }
      }
      .groupBy {
        case (_, timestamp, _) => timestamp
      }

    val res = x
      .mapValues(y =>
        y.map {
          case (facetValue, _, count) => (facetValue, count)
        }.toMap
      )
      .toSeq
      .map {
        case (timestamp, data) =>
          val others = globalRange(timestamp) - data.values.sum
          CountEntry(timestamp, data + ("_others_" -> others))
      }

    res
  }

  private def telemetryCounts(startTime: Instant, endTime: Instant, limit: Int): Future[Seq[CountEntry]] = {
    val pivotField: PivotField =
      PivotField(field = "sourceId", sort = CountSort, limit = limit)

    client
      .runRangeFacet(
        collection = SolrTelemetryIndexModel.apply().name,
        timestampField = TIMESTAMP_FIELD,
        pivot = Seq(pivotField),
        startTime = startTime,
        endTime = endTime,
        gap = GAP
      )
      .map(mapPivot(_, pivotField))

  }

  private def eventCounts(startTime: Instant, endTime: Instant, limit: Int): Future[Seq[CountEntry]] = {
    val pivotField: PivotField =
      PivotField(field = "severity", sort = CountSort, limit = limit)
    client
      .runRangeFacet(
        collection = SolrEventIndex.apply().name,
        timestampField = TIMESTAMP_FIELD,
        pivot = Seq(pivotField),
        startTime = startTime,
        endTime = endTime,
        gap = GAP
      )
      .map(mapPivot(_, pivotField))
  }

  private def logCounts(startTime: Instant, endTime: Instant, limit: Int): Future[Seq[CountEntry]] = {
    val pivotField: PivotField =
      PivotField(field = "log_level", sort = CountSort, limit = limit)
    client
      .runRangeFacet(
        collection = SolrLoggerIndex.apply().name,
        timestampField = TIMESTAMP_FIELD,
        pivot = Seq(pivotField),
        startTime = startTime,
        endTime = endTime,
        gap = GAP
      )
      .map(mapPivot(_, pivotField))
  }

  override def counts(startTimestamp: Instant, endTimestamp: Instant, limit: Int): Future[Counts] = {

    val eventuallyTelemetry = telemetryCounts(startTimestamp, endTimestamp, limit)
    val eventuallyLogs      = logCounts(startTimestamp, endTimestamp, limit)
    val eventuallyEvents    = eventCounts(startTimestamp, endTimestamp, limit)

    for {
      telemetry <- eventuallyTelemetry
      logs      <- eventuallyLogs
      events    <- eventuallyEvents
    } yield Counts(logs = logs, telemetry = telemetry, events = events)

  }

}
