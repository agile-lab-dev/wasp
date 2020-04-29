package it.agilelab.bigdata.wasp.master.web.controllers

import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.{Date, function}

import it.agilelab.bigdata.wasp.core.{SolrLoggerIndex, models}
import it.agilelab.bigdata.wasp.core.models.LogEntry
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.impl.CloudSolrServer
import org.apache.solr.client.solrj.response.QueryResponse
import org.apache.solr.client.solrj.util.ClientUtils
import org.apache.solr.common.params.DefaultSolrParams

import scala.concurrent.{ExecutionContext, Future}

trait LogsService {
  def logs(search: String,
           startTimestamp: Instant,
           endTimestamp: Instant,
           page: Int,
           size: Int): Future[Seq[LogEntry]]

}

class DefaultSolrLogsService(implicit ec: ExecutionContext)
    extends LogsService {

  private val solr: ConcurrentHashMap[String, CloudSolrServer] =
    new ConcurrentHashMap[String, CloudSolrServer]()

  private val createSolrClient =
    new function.Function[String, CloudSolrServer] {
      override def apply(t: String): CloudSolrServer = new CloudSolrServer(t)
    }

  override def logs(search: String,
                    startTimestamp: Instant,
                    endTimestamp: Instant,
                    page: Int,
                    size: Int): Future[Seq[LogEntry]] = {

    val client = getOrCreateClient(
      ConfigManager.getSolrConfig.zookeeperConnections.toString
    )

    val query =
      s"timestamp:[${startTimestamp.toString} TO ${endTimestamp.toString}]" +
        s" AND all:*${ClientUtils.escapeQueryChars(search)}*"

    runPredicate(client, SolrLoggerIndex().name, query, size, page).map {
      response =>
        import scala.collection.JavaConverters._
        response.getResults.asScala.toList.map { document =>
          models.LogEntry(
            document.getFieldValue("log_source").asInstanceOf[String],
            document.getFieldValue("log_level").asInstanceOf[String],
            document.getFieldValue("message").asInstanceOf[String],
            document.getFieldValue("timestamp").asInstanceOf[Date].toInstant,
            document.getFieldValue("thread").asInstanceOf[String],
            Option(document.getFieldValue("cause").asInstanceOf[String]),
            Option(document.getFieldValue("stack_trace").asInstanceOf[String])
          )
        }

    }

  }

  def runPredicate(solrServer: CloudSolrServer,
                   collection: String,
                   predicate: String,
                   rows: Int,
                   page: Int): Future[QueryResponse] = {

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
