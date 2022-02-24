package it.agilelab.bigdata.wasp.master.web.controllers

import it.agilelab.bigdata.wasp.core.SolrLoggerIndex
import it.agilelab.bigdata.wasp.models
import it.agilelab.bigdata.wasp.models.Logs
import org.apache.solr.client.solrj.util.ClientUtils

import java.time.Instant
import java.util.Date
import scala.concurrent.{ExecutionContext, Future}

trait LogsService {
  def logs(search: String,
           startTimestamp: Instant,
           endTimestamp: Instant,
           page: Int,
           size: Int): Future[Logs]

}



class DefaultSolrLogsService(client: SolrClient)(implicit ec: ExecutionContext)
    extends LogsService {

  override def logs(search: String,
                    startTimestamp: Instant,
                    endTimestamp: Instant,
                    page: Int,
                    size: Int): Future[Logs] = {

    val stringQuery = if (search.trim.isEmpty) "*" else ClientUtils.escapeQueryChars(search)

    val query =
      s"timestamp:[${startTimestamp.toString} TO ${endTimestamp.toString}]" +
        s" AND all:${stringQuery}"

    client.runPredicate(SolrLoggerIndex().name, query, size, page).map {
      response =>
        val found = response.getResults.getNumFound
        import scala.collection.JavaConverters._
        Logs(
          found,
          entries = response.getResults.asScala.toList.map { document =>
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
        )

    }

  }

}
