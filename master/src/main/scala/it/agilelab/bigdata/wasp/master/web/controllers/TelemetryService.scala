package it.agilelab.bigdata.wasp.master.web.controllers

import java.time.{Duration, Instant}
import java.util.Date
import java.util.regex.Pattern

import it.agilelab.bigdata.wasp.core.SolrTelemetryIndexModel
import it.agilelab.bigdata.wasp.core.models.{MetricEntry, Metrics, SourceEntry, Sources, TelemetryPoint, TelemetrySeries}
import org.apache.solr.client.solrj.util.ClientUtils

import scala.concurrent.{ExecutionContext, Future}
import scala.collection.JavaConverters._

trait TelemetryService {
  def sources(search: String, size: Int): Future[Sources]

  def metrics(source: SourceEntry, search: String, size: Int): Future[Metrics]

  def values(sourceEntry: SourceEntry,
             metricEntry: MetricEntry,
             startTimestamp: Instant,
             endTimestamp: Instant,
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
          terms.asScala.map(_.getTerm.replaceAll(Pattern.quote(regex), "")).map(x => MetricEntry(source, x))
        )
    }
  }

  override def values(
                       sourceEntry: SourceEntry,
                       metricEntry: MetricEntry,
                       startTimestamp: Instant,
                       endTimestamp: Instant,
                       size: Int
                     ): Future[TelemetrySeries] = {

    val increment = Duration.between(startTimestamp, endTimestamp).dividedBy(size).toMillis

    val metricKey = ClientUtils.escapeQueryChars(s"${sourceEntry.name}|${metricEntry.name}")

    val search = s"timestamp:[${startTimestamp} TO ${endTimestamp}] AND metricSearchKey:$metricKey"

    val rowsPerQuery = 10000

    def query(current: Long, page: Int, accumulator: Map[Long, (Int, Double)]): Future[Map[Long, (Int, Double)]] = {
      solrClient.runPredicate(SolrTelemetryIndexModel().name, search, rowsPerQuery, page).flatMap { response =>

        val total = response.getResults.getNumFound

        val updatedCounts: Map[Long, (Int, Double)] = response.getResults.asScala.filter(d => d.containsKey("value")).foldLeft(accumulator) {
          case (acc, document) =>
            val timestamp = document.getFieldValue("timestamp").asInstanceOf[Date].toInstant
            val value = document.getFieldValue("value").asInstanceOf[Double]
            val bucket = Duration.between(startTimestamp, timestamp).toMillis / increment

            val toBeUpdated = accumulator.getOrElse(bucket, (0, 0.0)) match {
              case (count, total) => (count + 1, total + value)
            }

            acc + (bucket -> toBeUpdated)

        }

        if (total < current) {
          query(current + rowsPerQuery, page + 1, updatedCounts)
        } else {
          Future.successful(updatedCounts)
        }

      }
    }


    query(0, 0, Map.empty).map { result =>


      val series = result.toSeq.sortBy(_._1).map {
        case (bucket, (samples, sum)) =>

          val timestamp = startTimestamp.plus(Duration.ofMillis(increment * bucket))

          val avg = sum / samples.toDouble

          TelemetryPoint(timestamp, avg)
      }

      TelemetrySeries(sourceEntry, metricEntry, series)
    }


  }
}
