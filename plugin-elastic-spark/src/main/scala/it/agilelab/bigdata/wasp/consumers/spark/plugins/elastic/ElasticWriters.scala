package it.agilelab.bigdata.wasp.consumers.spark.plugins.elastic

import akka.actor.ActorRef
import it.agilelab.bigdata.wasp.consumers.spark.writers.{
  SparkStreamingWriter,
  SparkStructuredStreamingWriter,
  SparkWriter
}
import it.agilelab.bigdata.wasp.core.WaspSystem.??
import it.agilelab.bigdata.wasp.core.bl.IndexBL
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.IndexModel
import it.agilelab.bigdata.wasp.core.utils.{ConfigManager, ElasticConfiguration}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.elasticsearch.spark.sparkRDDFunctions
import org.elasticsearch.spark.sql.EsSparkSQL

class ElasticSparkStreamingWriter(indexBL: IndexBL,
                                  ssc: StreamingContext,
                                  id: String,
                                  elasticAdminActor: ActorRef)
    extends SparkStreamingWriter
    with ElasticConfiguration
    with Logging {

  override def write(stream: DStream[String]): Unit = {
    val indexOpt: Option[IndexModel] = indexBL.getById(id)
    if (indexOpt.isDefined) {
      val index = indexOpt.get
      val indexName = ConfigManager.buildTimedName(index.name)
      logger.info(
        s"Check or create the index model: '${index.toString} with this index name: $indexName")

      if (??[Boolean](elasticAdminActor,
                      CheckOrCreateIndex(indexName,
                                         index.name,
                                         index.dataType,
                                         index.getJsonSchema))) {
        val resourceBroadcast = ssc.sparkContext.broadcast(index.resource)
        val address = elasticConfig.connections
          .filter(
            _.metadata.flatMap(_.get("connectiontype")).getOrElse("") == "rest")
          .mkString(",")

        val optionsBroadcasted = ssc.sparkContext.broadcast(
          Map("es.nodes" -> address,
              "es.input.json" -> "true",
              "es.batch.size.entries" -> "1"))

        logger.info(
          s"Write to elastic with spark streaming. Configuration passed: options: ${optionsBroadcasted.value}, resource: ${resourceBroadcast.value}")

        stream.foreachRDD((rdd: RDD[String]) => {
          rdd.saveToEs(resourceBroadcast.value, optionsBroadcasted.value)
        })

      } else {
        logger.error(s"Error creating index $index")
        throw new Exception(s"Error creating index $index")
        //TODO handle errors
      }
    } else {
      logger.warn(
        s"The index '$id' does not exits pay ATTENTION spark won't start")

    }
  }
}

class ElasticSparkStructuredStreamingWriter(indexBL: IndexBL,
                                            ss: SparkSession,
                                            id: String,
                                            elasticAdminActor: ActorRef)
    extends SparkStructuredStreamingWriter
    with ElasticConfiguration
    with Logging {

  override def write(stream: DataFrame,
                     queryName: String,
                     checkpointDir: String): Unit = {

    val indexOpt: Option[IndexModel] = indexBL.getById(id)
    if (indexOpt.isDefined) {
      val index = indexOpt.get
      val indexName = ConfigManager.buildTimedName(index.name)

      logger.info(
        s"Check or create the index model: '${index.toString} with this index name: $indexName")

      if (index.schema.isEmpty) {
        throw new Exception(
          s"There no define schema in the index configuration: $index")
      }
      if (index.name.toLowerCase != index.name) {
        throw new Exception(s"The index name must be all lowercase: $index")
      }
      if (??[Boolean](elasticAdminActor,
        CheckOrCreateIndex(indexName,
          index.name,
          index.dataType,
          index.getJsonSchema))) {

        stream.writeStream
          .option("checkpointLocation", checkpointDir)
          .format("es")
          .queryName(queryName)
          .start()
      }

    } else {
      logger.warn(
        s"The index '$id' does not exits pay ATTENTION spark won't start")
    }

  }

}

class ElasticSparkWriter(indexBL: IndexBL,
                         sc: SparkContext,
                         id: String,
                         elasticAdminActor: ActorRef)
    extends SparkWriter
    with ElasticConfiguration
    with Logging {

  override def write(data: DataFrame): Unit = {

    val indexOpt: Option[IndexModel] = indexBL.getById(id)
    if (indexOpt.isDefined) {
      val index = indexOpt.get
      val indexName = ConfigManager.buildTimedName(index.name)

      logger.info(
        s"Check or create the index model: '${index.toString} with this index name: $indexName")

      if (index.schema.isEmpty) {
        //TODO Gestire meglio l'eccezione
        throw new Exception(
          s"There no define schema in the index configuration: $index")
      }
      if (index.name.toLowerCase != index.name) {
        //TODO Gestire meglio l'eccezione
        throw new Exception(s"The index name must be all lowercase: $index")
      }
      if (??[Boolean](elasticAdminActor,
                      CheckOrCreateIndex(indexName,
                                         index.name,
                                         index.dataType,
                                         index.getJsonSchema))) {

        val addressBroadcast = sc.broadcast(
          elasticConfig.connections
            .filter(
              _.metadata
                .flatMap(_.get("connectiontype"))
                .getOrElse("") == "binary")
            .mkString(","))

        //TODO perchè togliendo la parte commentata la scrittura fallisce?
        val options = Map(
          "es.nodes" -> addressBroadcast.value,
          /* "es.input.json" -> "true",*/ "es.batch.size.entries" -> "1")
        logger.info(s"Data schema: ${data.schema}")
        logger.info(
          s"Write to elastic with this configuration: options: $options, resource: ${index.resource}")

        EsSparkSQL.saveToEs(data, index.resource, options)
      } else {
        logger.error(s"Error creating index $index")
        throw new Exception(s"Error creating index $index")
        //TODO handle errors
      }
    } else {
      logger.warn(
        s"The index '$id' does not exits pay ATTENTION spark won't start")

    }
  }
}