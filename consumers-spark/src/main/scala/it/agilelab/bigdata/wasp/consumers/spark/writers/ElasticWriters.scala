package it.agilelab.bigdata.wasp.consumers.spark.writers

import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.WaspSystem._
import it.agilelab.bigdata.wasp.core.bl.IndexBL
import it.agilelab.bigdata.wasp.core.elastic.CheckOrCreateIndex
import it.agilelab.bigdata.wasp.core.utils.{BSONFormats, ConfigManager, ElasticConfiguration}
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.elasticsearch.spark.sparkRDDFunctions
import org.elasticsearch.spark.sql.EsSparkSQL

import scala.concurrent.Await


class ElasticSparkStreamingWriter(env: {val indexBL: IndexBL},
                                  ssc: StreamingContext,
                                  id: String)
  extends SparkStreamingWriter with ElasticConfiguration {

  override def write(stream: DStream[String]): Unit = {

    val indexFut = env.indexBL.getById(id)
    val indexOpt = Await.result(indexFut, timeout.duration)
    indexOpt.foreach(index => {

      val indexName = ConfigManager.buildTimedName(index.name)
      if (??[Boolean](WaspSystem.elasticAdminActor, CheckOrCreateIndex(indexName, index.name, index.dataType, BSONFormats.toString(index.schema.get)))) {
        val resourceBroadcast = ssc.sparkContext.broadcast(index.resource)
        val addressBroadcast = ssc.sparkContext.broadcast(elasticConfig.connections.filter(_.metadata.flatMap(_.get("connectiontype")).getOrElse("") == "rest").mkString(","))

        stream.foreachRDD(rdd => rdd.saveToEs(resourceBroadcast.value, Map("es.nodes" -> addressBroadcast.value, "es.input.json" -> "true", "es.batch.size.entries" -> "1")))
      } else {
        throw new Exception("Error creating index " + index.name)
        //TODO handle errors
      }
    })
  }
}

class ElasticSparkWriter(env: {val indexBL: IndexBL},
                         sc: SparkContext,
                         id: String)
  extends SparkWriter with ElasticConfiguration {
  
  override def write(data: DataFrame): Unit = {
    val indexFut = env.indexBL.getById(id)
    val indexOpt = Await.result(indexFut, timeout.duration)
    indexOpt.foreach(index => {
      
      val indexName = ConfigManager.buildTimedName(index.name)
      if (index.schema.isEmpty) {
        //TODO Gestire meglio l'eccezione
        throw new Exception(s"There no define schema in the index configuration: $index")
      }
      if (index.name.toLowerCase != index.name) {
        //TODO Gestire meglio l'eccezione
        throw new Exception(s"The index name must be all lowercase: $index")
      }
      if (??[Boolean](WaspSystem.elasticAdminActor, CheckOrCreateIndex(indexName, index.name, index.dataType, BSONFormats.toString(index.schema.get)))) {
        println(data.count())
        data.printSchema()
        val resourceBroadcast = sc.broadcast(index.resource)
        val addressBroadcast = sc.broadcast(elasticConfig.connections.filter(_.metadata.flatMap(_.get("connectiontype")).getOrElse("") == "binary").mkString(","))

        //TODO perchè togliendo la parte commentata la scrittura fallisce?
        EsSparkSQL.saveToEs(data, index.resource, Map("es.nodes" -> addressBroadcast.value,/* "es.input.json" -> "true",*/ "es.batch.size.entries" -> "1"))
      } else {
        throw new Exception("Error creating index " + index.name)
        //TODO handle errors
      }
      
      
    })
  }
}
