package it.agilelab.bigdata.wasp.consumers.spark.readers

import it.agilelab.bigdata.wasp.core.bl.IndexBL
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.IndexModel
import it.agilelab.bigdata.wasp.core.utils.{ConfigManager, ElasticConfiguration, SolrConfiguration}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.elasticsearch.spark.sql.EsSparkSQL
import org.elasticsearch.hadoop.cfg.ConfigurationOptions


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
class ElasticIndexReader(indexModel: IndexModel) extends StaticReader with ElasticConfiguration with Logging {
  val name: String = indexModel.name
  val readerType: String = IndexModel.readerType

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
class SolrIndexReader(indexModel: IndexModel) extends StaticReader with SolrConfiguration with Logging {
  val name: String = indexModel.name
  val readerType: String = IndexModel.readerType

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
  
  def create(indexBL: IndexBL, id: String, name: String): Option[StaticReader] = {

    val defaultDataStoreIndexed = ConfigManager.getWaspConfig.defaultIndexedDatastore
  
    val indexOpt = indexBL.getById(id)
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
