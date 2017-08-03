package it.agilelab.bigdata.wasp.consumers.spark.readers

import com.lucidworks.spark.SolrRDD
import com.typesafe.config.{Config, ConfigFactory}
import it.agilelab.bigdata.wasp.consumers.spark.SparkHolder
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.WaspSystem._
import it.agilelab.bigdata.wasp.core.bl.IndexBL
import it.agilelab.bigdata.wasp.core.logging.WaspLogger
import it.agilelab.bigdata.wasp.core.models.IndexModel
import it.agilelab.bigdata.wasp.core.utils.{ElasticConfiguration, SolrConfiguration}
import org.apache.solr.client.solrj.SolrQuery
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.elasticsearch.spark.sql.EsSparkSQL
import org.elasticsearch.hadoop.cfg.ConfigurationOptions

import scala.concurrent.Await
import scala.tools.nsc.util.ClassPath.JavaContext

/**
 * Created by Mattia Bertorello on 10/09/15.
 */

trait StaticReader {
  val name: String
  val readerType: String

  def read(sc: SparkContext): DataFrame
}

/**
 * It read data from Elastic with the configuration of ElasticConfiguration.
 * It use the push down method of SparkSQL to convert SQL to elastic query
  *
  * @param indexModel Elastic configuration
 */
class ElasticIndexReader(indexModel: IndexModel) extends StaticReader with ElasticConfiguration {
  val name: String = indexModel.name
  val readerType: String = IndexModel.readerType
  private val logger = WaspLogger(this.getClass.getName)

  override def read(sc: SparkContext): DataFrame = {

    val sqlContext = new SQLContext(sc)
    val options = Map(
      "pushdown" -> "true",
      ConfigurationOptions.ES_NODES -> elasticConfig.connections.map(_.toString).mkString(" "),
      ConfigurationOptions.ES_RESOURCE_READ -> indexModel.resource
    )

    val optionsWithQuery = indexModel.query match {
      case Some(query) => options + (ConfigurationOptions.ES_QUERY -> query)
      case None => options
    }
    logger.info(s"Elastic options: $optionsWithQuery")
    EsSparkSQL.esDF(sqlContext, optionsWithQuery)
  }

}

/**
  * It read data from Solr with the configuration of SolrConfiguration.
  * It use the push down method of SparkSQL to convert SQL to elastic query
  *
  * @param indexModel Solr configuration
  */
class SolrIndexReader(indexModel: IndexModel) extends StaticReader with SolrConfiguration {
  val name: String = indexModel.name
  val readerType: String = IndexModel.readerType
  private val logger = WaspLogger(this.getClass.getName)

  override def read(sc: SparkContext): DataFrame = ???
  /* {

    ***
    *** This implementation work only with Solr 5.5.1 and com.lucidworks.spark.spark-solr 2.0
    ***

    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val options = Map(
      "collection" -> s"${indexModel.collection}",
      "zkhost" -> s"""${solrConfig.connections.map(_.toString).mkString("")}"""
    )
    val df = sqlContext.read.format("solr")
      .options(options)
      .load

    df
  } */
}

object IndexReader {

  val conf: Config = ConfigFactory.load

  def create(indexBL: IndexBL, id: String, name: String): Option[StaticReader] = {

    val defaultDataStoreIndexed = conf.getString("default.datastore.indexed")

    val indexFut = indexBL.getById(id)
    val indexOpt = Await.result(indexFut, timeout.duration)
    if (indexOpt.isDefined) {
      defaultDataStoreIndexed match {
        case "elastic" => Some(new ElasticIndexReader(indexOpt.get))
        case "solr" => Some(new SolrIndexReader(indexOpt.get))
        case _ => Some(new ElasticIndexReader(indexOpt.get))
      }
    } else {
      None
    }
  }

}
