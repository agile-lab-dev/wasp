package it.agilelab.bigdata.wasp.consumers.spark.plugins.solr

import it.agilelab.bigdata.wasp.consumers.spark.readers.SparkReader
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.IndexModel
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
  val readerType: String = IndexModel.readerType

  override def read(sc: SparkContext): DataFrame = {

    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val options = Map(
      "collection" -> s"${indexModel.collection}",
      "zkHost" -> solrZkHost
    )
    val df = sqlContext.read.format("solr")
      .options(options)
      .load

    df
  }
}
