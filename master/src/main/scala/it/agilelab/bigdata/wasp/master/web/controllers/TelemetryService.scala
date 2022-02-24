package it.agilelab.bigdata.wasp.master.web.controllers

import it.agilelab.bigdata.wasp.core.SolrTelemetryIndexModel
import it.agilelab.bigdata.wasp.models._
import org.apache.solr.client.solrj.util.ClientUtils
import org.apache.solr.common.util.{NamedList, SimpleOrderedMap}

import java.time.{Duration, Instant}
import java.util.Date
import java.util.regex.Pattern
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

trait TelemetryService {
  def sources(search: String, size: Int): Future[Sources]

  def metrics(source: SourceEntry, search: String, size: Int): Future[Metrics]

  def values(
      sourceEntry: SourceEntry,
      metricEntry: MetricEntry,
      startTimestamp: Instant,
      endTimestamp: Instant,
      aggregate: Aggregate.Aggregate,
      size: Int
  ): Future[TelemetrySeries]

}

class DefaultSolrTelemetryService(solrClient: SolrClient)(implicit ec: ExecutionContext) extends TelemetryService {

  override def sources(search: String, size: Int): Future[Sources] = {
    solrClient.runTermQuery(SolrTelemetryIndexModel.apply().name, "sourceId", "", search, size).map { response =>
      val terms = response.getTermsResponse.getTerms("sourceId")
      Sources(terms.size(), terms.asScala.map(_.getTerm).map(x => SourceEntry(x)))
    }
  }

  override def metrics(source: SourceEntry, search: String, size: Int): Future[Metrics] = {

    val regex = source.name + "|" + search

    solrClient.runTermQuery(SolrTelemetryIndexModel.apply().name, "metricSearchKey", source.name, regex, size).map {
      response =>
        val terms = response.getTermsResponse.getTerms("metricSearchKey")
        Metrics(
          terms.size(),
          terms.asScala.map(_.getTerm.replaceAll(Pattern.quote(source.name + "|"), "")).map(x => MetricEntry(source, x))
        )
    }
  }

  override def values(
      sourceEntry: SourceEntry,
      metricEntry: MetricEntry,
      startTimestamp: Instant,
      endTimestamp: Instant,
      aggregate: Aggregate.Aggregate,
      size: Int
  ): Future[TelemetrySeries] = {

    val increment = Duration.between(startTimestamp, endTimestamp).dividedBy(size)

    val metricKey = ClientUtils.escapeQueryChars(s"${sourceEntry.name}|${metricEntry.name}")

    val predicate = s"metricSearchKey:$metricKey"

    solrClient
      .runBucketAggregation(
        collection = SolrTelemetryIndexModel.apply().name,
        predicate = predicate,
        timestampField = "timestamp",
        valueField = "value",
        aggregateFunction = aggregate.toString,
        startTime = startTimestamp,
        endTime = endTimestamp,
        increment = increment
      )
      .map { response =>
        val maybeMaxValue = Option(
          response.getResponse
            .get("facets")
            .asInstanceOf[NamedList[Any]]
            .get(s"${aggregate.toString}_value")
            .asInstanceOf[NamedList[Any]]
        )

        val maybeBucket = maybeMaxValue.map(_.get("buckets").asInstanceOf[java.util.ArrayList[SimpleOrderedMap[Any]]])

        val points = maybeBucket
          .map { bucket =>
            bucket.asScala.map { entry =>
              val timestamp = entry.get("val").asInstanceOf[Date].toInstant
              val result: java.lang.Double =
                Option(entry.get(aggregate.toString).asInstanceOf[java.lang.Double]).getOrElse(java.lang.Double.NaN)

              TelemetryPoint(timestamp, result)
            }.toList
          }
          .getOrElse(List.empty)

        TelemetrySeries(sourceEntry, metricEntry, points)
      }

  }
}
