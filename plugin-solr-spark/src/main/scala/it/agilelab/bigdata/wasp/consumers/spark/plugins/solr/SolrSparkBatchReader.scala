package it.agilelab.bigdata.wasp.consumers.spark.plugins.solr

import it.agilelab.bigdata.solr.SolrDataframe
import it.agilelab.bigdata.wasp.consumers.spark.readers.SparkBatchReader
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct.SolrProduct
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.utils.SolrConfiguration
import it.agilelab.bigdata.wasp.models.IndexModel
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

/**
  * It read data from Solr with the configuration of SolrConfiguration.
  * It use the push down method of SparkSQL to convert SQL to elastic query
  *
  * @param indexModel Solr configuration
  */
class SolrSparkBatchReader(indexModel: IndexModel) extends SparkBatchReader with SolrConfiguration with Logging {
  val name: String = indexModel.name
  val readerType: String = SolrProduct.getActualProductName

  override def read(sc: SparkContext): DataFrame = {

    val sparkSession = SparkSession.builder().getOrCreate()

    new SolrDataframe(sparkSession, solrConfig.zookeeperConnections.toString, indexModel.name).df
  }
}
