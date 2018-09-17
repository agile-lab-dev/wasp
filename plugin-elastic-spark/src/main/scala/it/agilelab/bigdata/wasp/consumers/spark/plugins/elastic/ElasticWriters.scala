package it.agilelab.bigdata.wasp.consumers.spark.plugins.elastic

import akka.actor.ActorRef
import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkLegacyStreamingWriter, SparkStructuredStreamingWriter, SparkBatchWriter}
import it.agilelab.bigdata.wasp.core.WaspSystem.??
import it.agilelab.bigdata.wasp.core.bl.IndexBL
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.IndexModel
import it.agilelab.bigdata.wasp.core.utils.ElasticConfiguration
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.elasticsearch.spark.sparkRDDFunctions
import org.elasticsearch.spark.sql.EsSparkSQL

class ElasticsearchSparkLegacyStreamingWriter(indexBL: IndexBL,
                                              ssc: StreamingContext,
                                              name: String,
                                              elasticAdminActor: ActorRef)
    extends SparkLegacyStreamingWriter
    with ElasticConfiguration
    with Logging {

  override def write(stream: DStream[String]): Unit = {

    val indexOpt: Option[IndexModel] = indexBL.getByName(name)
    if (indexOpt.isDefined) {
      val index = indexOpt.get
      val indexName = index.eventuallyTimedName
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

        val options = Map("es.nodes" -> address,
                          "es.input.json" -> "true",
                          "es.batch.size.entries" -> "1") ++ indexOpt.get.idField.map(it => ("es.mapping.id", it))

        val optionsBroadcasted = ssc.sparkContext.broadcast(options)

        logger.info(
          s"Write to elastic with spark streaming. Configuration passed: options: ${optionsBroadcasted.value}, resource: ${resourceBroadcast.value}")

        stream.foreachRDD((rdd: RDD[String]) => {
          rdd.saveToEs(resourceBroadcast.value, optionsBroadcasted.value)
        })

      } else {
        val msg = s"Error creating index $index"
        logger.error(msg)
        throw new Exception(msg)
      }
    } else {
      logger.warn(s"The index '$name' does not exits pay ATTENTION spark won't start")
    }
  }
}

class ElasticsearchSparkStructuredStreamingWriter(indexBL: IndexBL,
                                                  ss: SparkSession,
                                                  name: String,
                                                  elasticAdminActor: ActorRef)
    extends SparkStructuredStreamingWriter
    with ElasticConfiguration
    with Logging {

  override def write(stream: DataFrame): DataStreamWriter[Row] = {

    val indexOpt: Option[IndexModel] = indexBL.getByName(name)
    if (indexOpt.isDefined) {
      val index = indexOpt.get
      val indexName = index.eventuallyTimedName
      val resource = index.resource

      logger.info(
        s"Check or create the index model: '${index.toString} with this index name: $indexName")

      if (index.schema.isEmpty) {
        throw new Exception(
          s"There no define schema in the index configuration: $index")
      }
      if (index.name.toLowerCase != index.name) {
        throw new Exception(s"The index name must be all lowercase: $index")
      }

      val options = indexOpt.get.idField.map(it => ("es.mapping.id", it)).toMap + ("path" -> resource)

      if (??[Boolean](
          elasticAdminActor,
        CheckOrCreateIndex(
          indexName,
          index.name,
          index.dataType,
          index.getJsonSchema))) {

        stream
          .writeStream
          .options(options)
          .format("es")
      } else {
        val msg = s"Error creating elastic index: $index with this index name $indexName"
        logger.error(msg)
        throw new Exception(msg)
      }
    } else {
      val message = s"The index '$name' does not exits pay ATTENTION spark won't start"
      logger.error(message)
      throw new Exception(message)

    }
  }

}

class ElasticsearchSparkBatchWriter(indexBL: IndexBL,
                                    sc: SparkContext,
                                    name: String,
                                    elasticAdminActor: ActorRef)
    extends SparkBatchWriter
    with ElasticConfiguration
    with Logging {

  override def write(data: DataFrame): Unit = {

    val indexOpt: Option[IndexModel] = indexBL.getByName(name)
    if (indexOpt.isDefined) {
      val index = indexOpt.get
      val indexName = index.eventuallyTimedName

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
          /* "es.input.json" -> "true",*/
          "es.batch.size.entries" -> "1") ++ indexOpt.get.idField.map(it => ("es.mapping.id", it))

        logger.info(s"Data schema: ${data.schema}")
        logger.info(
          s"Write to elastic with this configuration: options: $options, resource: ${index.resource}")

        EsSparkSQL.saveToEs(data, index.resource, options)
      } else {
        val msg = s"Error creating index $index"
        logger.error(msg)
        throw new Exception(msg)
      }
    } else {
      logger.warn(s"The index '$name' does not exits pay ATTENTION spark won't start")
    }
  }
}
