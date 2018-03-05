package it.agilelab.bigdata.wasp.consumers.spark.plugins.solr

import it.agilelab.bigdata.solr.SolrDataframe
import it.agilelab.bigdata.wasp.consumers.spark.readers.SparkReader
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.{Datastores, IndexModel}
import it.agilelab.bigdata.wasp.core.utils.SolrConfiguration
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * It read data from Solr with the configuration of SolrConfiguration.
  * It use the push down method of SparkSQL to convert SQL to elastic query
  *
  * @param indexModel Solr configuration
  */
class SolrSparkReader(indexModel: IndexModel) extends SparkReader with SolrConfiguration with Logging {
  val name: String = indexModel.name
  val readerType: String = Datastores.solrProduct

  override def read(sc: SparkContext): DataFrame = {

    //Fast Workaround to get sparkSession
    val sqlContext = new SQLContext(sc)
    val sparkSession = sqlContext.sparkSession

    new SolrDataframe(sparkSession, solrConfig.zookeeperConnections.toString, indexModel.name).df
  }
}
