package it.agilelab.bigdata.wasp.consumers.spark.plugins.solr

import java.util

import akka.actor.ActorRef
import com.lucidworks.spark.SolrSupport
import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkLegacyStreamingWriter, SparkStructuredStreamingWriter, SparkWriter}
import it.agilelab.bigdata.wasp.core.WaspSystem.??
import it.agilelab.bigdata.wasp.core.bl.IndexBL
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.IndexModel
import it.agilelab.bigdata.wasp.core.utils.{ConfigManager, SolrConfiguration}
import org.apache.solr.client.solrj.impl.CloudSolrServer
import org.apache.solr.common.SolrInputDocument
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.JavaDStream
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by matbovet on 02/09/2016.
  */
object SolrSparkWriter {

  def createSolrDocument(r: Row) = {
    val doc: SolrInputDocument = new SolrInputDocument()
    doc.setField("id", java.util.UUID.randomUUID.toString)

    val fieldname = r.schema.fieldNames
    fieldname.foreach { f =>

      if (!r.isNullAt(r.fieldIndex(f)))
        doc.setField(f, r.getAs(f))

    }

    doc
  }

}

class SolrSparkLegacyStreamingWriter(indexBL: IndexBL,
                                     ssc: StreamingContext,
                                     id: String,
                                     solrAdminActor: ActorRef)
    extends SparkLegacyStreamingWriter
    with SolrConfiguration
    with Logging {

  override def write(stream: DStream[String]): Unit = {

    val sqlContext = SQLContext.getOrCreate(ssc.sparkContext)
    val indexOpt: Option[IndexModel] = indexBL.getById(id)
    if (indexOpt.isDefined) {
      val index = indexOpt.get
      val indexName = index.eventuallyTimedName

      logger.info(s"Check or create the index model: '${index.toString} with this index name: $indexName")

      if (??[Boolean](
            solrAdminActor,
            CheckOrCreateCollection(
              indexName,
              index.getJsonSchema,
              index.numShards.getOrElse(1),
              index.replicationFactor.getOrElse(1)))) {

        val docs: DStream[SolrInputDocument] = stream.transform { rdd =>
          val df: Dataset[Row] = sqlContext.read.json(rdd)

          df.rdd.map { r =>
            SolrSparkWriter.createSolrDocument(r)
          }
        }

        SolrSupport.indexDStreamOfDocs(solrConfig.zookeeperConnections.getZookeeperConnection(),
                                       indexName,
                                       100,
                                       new JavaDStream[SolrInputDocument](docs))

      } else {
        val msg = s"Error creating solr index: $index with this index name $indexName"
        logger.error(msg)
        throw new Exception(msg)
      }
    } else {
      logger.warn(s"The index '$id' does not exits pay ATTENTION the spark stream won't start")
    }
  }

}

class SolrSparkStructuredStreamingWriter(indexBL: IndexBL,
                                         ss: SparkSession,
                                         id: String,
                                         solrAdminActor: ActorRef)
    extends SparkStructuredStreamingWriter
    with SolrConfiguration
    with Logging {

  override def write(stream: DataFrame,
                     queryName: String,
                     checkpointDir: String): Unit = {

    // get index model from BL
    val indexOpt: Option[IndexModel] = indexBL.getById(id)
    if (indexOpt.isDefined) {
      val index = indexOpt.get
      val indexName = index.eventuallyTimedName

      logger.info(
        s"Check or create the index model: '${index.toString} with this index name: ${index.name}")

      if (??[Boolean](
            solrAdminActor,
            CheckOrCreateCollection(
              indexName,
              index.getJsonSchema,
              index.numShards.getOrElse(1),
              index.replicationFactor.getOrElse(1))
          )) {

        val solrWriter = new SolrForeachWriter(
          ss,
          solrConfig.zookeeperConnections.getZookeeperConnection(),
          indexName,
          index.collection)


        stream.writeStream
          .option("checkpointLocation", checkpointDir)
          .foreach(solrWriter)
          .queryName(queryName)
          .start()

      } else {
        val msg = s"Error creating solr index: $index with this index name ${indexName}"
        logger.error(msg)
        throw new Exception(msg)
      }
    } else {
      logger.warn(s"The index '$id' does not exits pay ATTENTION the spark stream won't start")
    }

  }

}

class SolrForeachWriter(val ss: SparkSession,
                        val connection: String,
                        val indexName: String,
                        val collection: String)
    extends ForeachWriter[Row] {

  var solrServer: CloudSolrServer = _
  var batch: util.ArrayList[SolrInputDocument] = _
  val batchSize = 100

  override def open(partitionId: Long, version: Long): Boolean = {

    /*
    * new CloudSolrServer(solrConfig.connections.map(conn => s"${conn.host}:${conn.port}")
      .mkString(",") + "/solr")*/

    solrServer = SolrSupport.getSolrServer(connection)
    batch = new util.ArrayList[SolrInputDocument]
    true
  }

  override def process(value: Row): Unit = {
    val docs: SolrInputDocument = SolrSparkWriter.createSolrDocument(value)
    batch.add(docs)

    if (batch.size() > batchSize) {
      SolrSupport.sendBatchToSolr(solrServer, collection, batch)
    }
  }

  override def close(errorOrNull: Throwable): Unit = {
    if (batch.size() > 0) {
      SolrSupport.sendBatchToSolr(solrServer, collection, batch)
    }
  }
}

class SolrSparkWriter(indexBL: IndexBL,
                      sc: SparkContext,
                      id: String,
                      solrAdminActor: ActorRef)
    extends SparkWriter
    with SolrConfiguration
    with Logging {

  override def write(data: DataFrame): Unit = {

    val indexOpt: Option[IndexModel] = indexBL.getById(id)
    if (indexOpt.isDefined) {
      val index = indexOpt.get
      val indexName = index.eventuallyTimedName

      logger.info(
        s"Check or create the index model: '${index.toString} with this index name: $indexName")

      if (??[Boolean](
            solrAdminActor,
            CheckOrCreateCollection(
              indexName,
              index.getJsonSchema,
              index.numShards.getOrElse(1),
              index.replicationFactor.getOrElse(1))
          )) {

        val docs = data.rdd.map { r =>
          SolrSparkWriter.createSolrDocument(r)
        }

        SolrSupport.indexDocs(solrConfig.zookeeperConnections.getZookeeperConnection(),
                              indexName,
                              100,
                              new JavaRDD[SolrInputDocument](docs))

      } else {
        val msg = s"Error creating solr index ${indexName}"
        throw new Exception(msg)
      }

    } else {
      logger.warn(s"The index '$id' does not exits pay ATTENTION spark won't start")
    }

  }

}
