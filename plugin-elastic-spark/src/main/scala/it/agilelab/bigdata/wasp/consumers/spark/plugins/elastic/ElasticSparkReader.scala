package it.agilelab.bigdata.wasp.consumers.spark.plugins.elastic

import it.agilelab.bigdata.wasp.consumers.spark.readers.SparkReader
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.IndexModel
import it.agilelab.bigdata.wasp.core.utils.ElasticConfiguration
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.spark.sql.EsSparkSQL


/**
  * It read data from Elastic with the configuration of ElasticConfiguration.
  * It use the push down method of SparkSQL to convert SQL to elastic query
  *
  * @param indexModel Elastic configuration
  */
class ElasticSparkReader(indexModel: IndexModel) extends SparkReader with ElasticConfiguration with Logging {
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
    logger.info(s"Read from Elastic with this options: $optionsWithQuery and this model: $indexModel")
    EsSparkSQL.esDF(sqlContext, optionsWithQuery)
  }

}
