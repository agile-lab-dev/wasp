package it.agilelab.bigdata.wasp.master.web.controllers

import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.{Optional, function}
import java.util.regex.Pattern

import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.client.solrj.response.QueryResponse

import scala.concurrent.{ExecutionContext, Future}
import scala.collection.JavaConverters._
import java.time.Duration

import it.agilelab.bigdata.wasp.models.configuration.SolrConfigModel

sealed trait PivotSort
case object CountSort extends PivotSort
case object IndexSort extends PivotSort

case class PivotField(field: String, sort: PivotSort, limit: Int)

class SolrClient(config: SolrConfigModel)(implicit ec: ExecutionContext) {
  private val solr: ConcurrentHashMap[SolrConfigModel, CloudSolrClient] =
    new ConcurrentHashMap[SolrConfigModel, CloudSolrClient]()
  private val createSolrClient =
    new function.Function[SolrConfigModel, CloudSolrClient] {
      override def apply(t: SolrConfigModel): CloudSolrClient =
        new CloudSolrClient.Builder(
          t.zookeeperConnections.connections.map(c => s"${c.host}:${c.port}").toList.asJava,
          Optional.of(config.zookeeperConnections.chRoot)
        ).build()
    }

  def runPredicate(collection: String, predicate: String, rows: Int, page: Int): Future[QueryResponse] = {

    val solrServer = getOrCreateClient(config)
    val query      = new SolrQuery(predicate)
    val start      = rows * page
    query.setParam("collection", collection)
    query.setRows(rows)
    query.setStart(start)
    Future {
      solrServer.query(query)
    }
  }

  private def getOrCreateClient(config: SolrConfigModel) =
    solr.computeIfAbsent(config, createSolrClient)

  def runTermQuery(
      collection: String,
      field: String,
      prefix: String,
      regex: String,
      rows: Int
  ): Future[QueryResponse] = {
    val solrServer = getOrCreateClient(config)
    val query      = new SolrQuery()
    query.setParam("collection", collection)
    query.setRequestHandler("/terms")
    query.setTermsSortString("count")
    query.setTermsPrefix(prefix)
    query.setTermsRegex(".*" + Pattern.quote(regex) + ".*")
    query.addTermsField(field)
    query.setTermsLimit(rows)
    Future {
      solrServer.query(query)
    }
  }

  def runBucketAggregation(
      collection: String,
      predicate: String,
      timestampField: String,
      valueField: String,
      aggregateFunction: String,
      startTime: Instant,
      endTime: Instant,
      increment: Duration
  ): Future[QueryResponse] = {

    val hours   = increment.getSeconds / 3600
    val minutes = ((increment.getSeconds % 3600) / 60).toInt
    val secs    = (increment.getSeconds % 60).toInt

    val gap = s"+${hours}HOURS+${minutes}MINUTES+${secs}SECONDS"

    val jsonFacet = s"""{
                      |  "${aggregateFunction}_${valueField}": {
                      |    "type":"range",
                      |    "field":"$timestampField",
                      |    "start":"$startTime",
                      |    "end":"$endTime",
                      |    "gap":"$gap",
                      |    "facet": {
                      |        "${aggregateFunction}":"${aggregateFunction}(${valueField})"
                      |    }
                      |  }
                      |}""".stripMargin.replaceAll("\\s|\\n|\\r", "")

    val solrClient = getOrCreateClient(config)

    val query = new SolrQuery()

    query.setParam("collection", collection)
    query.setRows(0)

    query.setQuery(s"${predicate} AND ${timestampField}:[$startTime TO $endTime]")
    query.setParam("json.facet", jsonFacet)

    Future {
      solrClient.query(query)
    }
  }

  def runRangeFacet(
      collection: String,
      timestampField: String,
      pivot: Seq[PivotField],
      startTime: Instant,
      endTime: Instant,
      gap: String
  ): Future[QueryResponse] = {
    val rangeTag   = "r1"
    val solrServer = getOrCreateClient(config)
    val query      = new SolrQuery()

    query.setParam("collection", collection)
    query.setFacet(true)
    query.setQuery(s"${timestampField}:[$startTime TO $endTime]")
    query.setParam("facet.range", s"{!tag=$rangeTag}$timestampField")
    query.setParam("facet.range.start", startTime.toString)
    query.setParam("facet.range.end", endTime.toString)
    query.setParam("facet.range.gap", gap)
    query.setRows(0)

    if (pivot.nonEmpty) {
      query.setParam("facet.pivot", s"{!range=$rangeTag}${pivot.map(_.field).mkString(",")}")
    }

    pivot.foreach {
      case PivotField(name, CountSort, count) =>
        query.setParam(s"f.$name.facet.limit", count.toString)
        query.setParam(s"f.$name.facet.sort", "count")
      case PivotField(name, IndexSort, count) =>
        query.setParam(s"f.$name.facet.limit", count.toString)
        query.setParam(s"f.$name.facet.sort", "index")

    }

    Future {
      solrServer.query(query)
    }
  }
}

object SolrClient {
  def apply(config: SolrConfigModel)(implicit ec: ExecutionContext): SolrClient = new SolrClient(config)(ec)
}
