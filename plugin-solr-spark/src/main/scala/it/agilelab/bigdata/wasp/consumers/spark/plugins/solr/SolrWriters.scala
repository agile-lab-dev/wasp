package it.agilelab.bigdata.wasp.consumers.spark.plugins.solr

import java.util

import akka.actor.ActorRef
import com.lucidworks.spark.SolrSupport
import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkLegacyStreamingWriter, SparkStructuredStreamingWriter, SparkWriter}
import it.agilelab.bigdata.wasp.core.WaspSystem.??
import it.agilelab.bigdata.wasp.core.bl.IndexBL
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.IndexModel
import it.agilelab.bigdata.wasp.core.utils.SolrConfiguration
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

  def createSolrDocument(r: Row, idFieldOption: Option[String]) = {
    val doc: SolrInputDocument = new SolrInputDocument()

    // set the id of the document
    val id = idFieldOption match {
      case Some(idField) =>
        // we were told to use a specific field; let's extract the value from the row
        try {
          r.getAs[String](idField)
        } catch {
          case e: Exception =>
            val msg = s"Error retrieving idField '$idField'"
            throw new Exception(msg, e)
        }
      case None =>
        // we don't have a value to use; let's generate an id
        java.util.UUID.randomUUID.toString
    }
    doc.setField("id", id)

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

        val docs = stream.transform { rdd =>

          val df: Dataset[Row] = sqlContext.read.json(rdd)

          df.rdd.map { r =>
            try {
                SolrSparkWriter.createSolrDocument(r, index.idField)
            } catch {
              case e: Exception =>
                val msg = s"Unable to create Solr document. Error message: ${e.getMessage}"
                logger.error(msg)
                throw new Exception(msg, e)
            }
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

      logger.info(s"Check or create the index model: '${index.toString} with this index name: ${index.name}")

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
          index.collection,
          index.idField)

        stream.writeStream
          .option("checkpointLocation", checkpointDir)
          .foreach(solrWriter)
          .queryName(queryName)
          .start()

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

class SolrForeachWriter(ss: SparkSession,
                        connection: String,
                        collection: String,
                        idFieldOption: Option[String])
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

    try {
      val docs = SolrSparkWriter.createSolrDocument(value, idFieldOption)
      batch.add(docs)

      if (batch.size() > batchSize) {
        SolrSupport.sendBatchToSolr(solrServer, collection, batch)
      }
    } catch {
      case e: Exception =>
        val msg = s"Unable to create Solr document. Error message: ${e.getMessage}"
        //logger.error(msg) // Logging cannot be extended by SolrForeachWriter due to creates Serialization issue
        throw new Exception(msg, e)
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

      logger.info(s"Check or create the index model: '${index.toString} with this index name: $indexName")

      if (??[Boolean](
            solrAdminActor,
            CheckOrCreateCollection(
              indexName,
              index.getJsonSchema,
              index.numShards.getOrElse(1),
              index.replicationFactor.getOrElse(1))
          )) {

        val docs = data.rdd.map { r =>

          try {
            SolrSparkWriter.createSolrDocument(r, index.idField)
          } catch {
            case e: Exception =>
              val msg = s"Unable to create Solr document. Error message: ${e.getMessage}"
              logger.error(msg)
              throw new Exception(msg, e)
          }
        }

        SolrSupport.indexDocs(solrConfig.zookeeperConnections.getZookeeperConnection(),
                              indexName,
                              100,
                              new JavaRDD[SolrInputDocument](docs))

      } else {
        val msg = s"Error creating solr index: $index with this index name $indexName"
        logger.error(msg)
        throw new Exception(msg)
      }

    } else {
      logger.warn(s"The index '$id' does not exits pay ATTENTION spark won't start")
    }
  }
}