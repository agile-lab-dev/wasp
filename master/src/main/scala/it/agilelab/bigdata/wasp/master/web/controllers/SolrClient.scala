package it.agilelab.bigdata.wasp.master.web.controllers

import java.util.concurrent.ConcurrentHashMap
import java.util.function

import it.agilelab.bigdata.wasp.core.models.configuration.SolrConfigModel
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.impl.CloudSolrServer
import org.apache.solr.client.solrj.response.QueryResponse

import scala.concurrent.{ExecutionContext, Future}

class SolrClient(config: SolrConfigModel)(implicit ec: ExecutionContext) {
  private val solr: ConcurrentHashMap[String, CloudSolrServer] =
    new ConcurrentHashMap[String, CloudSolrServer]()
  private val createSolrClient =
    new function.Function[String, CloudSolrServer] {
      override def apply(t: String): CloudSolrServer = new CloudSolrServer(t)
    }

  def runPredicate(collection: String,
                   predicate: String,
                   rows: Int,
                   page: Int): Future[QueryResponse] = {

    val solrServer = getOrCreateClient(config.zookeeperConnections.toString)
    solrServer.setDefaultCollection(collection)
    val query = new SolrQuery(predicate)
    val start = rows * page
    query.setRows(rows)
    query.setStart(start)
    Future {
      solrServer.query(query)
    }
  }

  private def getOrCreateClient(zkString: String) =
    solr.computeIfAbsent(zkString, createSolrClient)
}


object SolrClient {
  def apply(config: SolrConfigModel)(implicit ec: ExecutionContext): SolrClient = new SolrClient(config)(ec)
}