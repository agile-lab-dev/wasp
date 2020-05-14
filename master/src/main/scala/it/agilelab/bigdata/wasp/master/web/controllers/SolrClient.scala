package it.agilelab.bigdata.wasp.master.web.controllers

import java.util.concurrent.ConcurrentHashMap
import java.util.{function, Optional}
import java.util.regex.Pattern

import it.agilelab.bigdata.wasp.core.models.configuration.SolrConfigModel
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.client.solrj.response.QueryResponse

import scala.concurrent.{ExecutionContext, Future}
import scala.collection.JavaConverters._

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
}

object SolrClient {
  def apply(config: SolrConfigModel)(implicit ec: ExecutionContext): SolrClient = new SolrClient(config)(ec)
}
