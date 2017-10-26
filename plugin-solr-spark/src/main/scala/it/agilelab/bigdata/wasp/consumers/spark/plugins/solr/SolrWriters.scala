package it.agilelab.bigdata.wasp.consumers.spark.plugins.solr

import akka.actor.ActorRef
import com.lucidworks.spark.SolrSupport
import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkStreamingWriter, SparkStructuredStreamingWriter, SparkWriter}
import it.agilelab.bigdata.wasp.core.WaspSystem.??
import it.agilelab.bigdata.wasp.core.bl.IndexBL
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.IndexModel
import it.agilelab.bigdata.wasp.core.utils.{ConfigManager, SolrConfiguration}
import org.apache.solr.common.SolrInputDocument
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.streaming.StreamingContext
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
      doc.setField(f, r.getAs(f))
    }

    doc
  }

}

class SolrSparkStreamingWriter(indexBL: IndexBL,
                               val ssc: StreamingContext,
                               val id: String,
                               solrAdminActor: ActorRef)
    extends SparkStreamingWriter
    with SolrConfiguration
    with Logging {

  override def write(stream: DStream[String]): Unit = {

    val sqlContext = SQLContext.getOrCreate(ssc.sparkContext)
    val indexOpt: Option[IndexModel] = indexBL.getById(id)
    if (indexOpt.isDefined) {
      val index = indexOpt.get
      val indexName = ConfigManager.buildTimedName(index.name)

      logger.info(
        s"Check or create the index model: '${index.toString} with this index name: $indexName")

      if (??[Boolean](
            solrAdminActor,
            CheckOrCreateCollection(
              indexName,
              index.schema.get.get("properties").asArray().toString,
              index.numShards.getOrElse(1),
              index.replicationFactor.getOrElse(1))
          )) {

        val docs: DStream[SolrInputDocument] = stream.transform { rdd =>
          val df: DataFrame = sqlContext.read.json(rdd)

          df.show()

          df.rdd.map { r =>
            SolrSparkWriter.createSolrDocument(r)
          }
        }

        SolrSupport.indexDStreamOfDocs(solrConfig.connections.mkString(","),
                                       indexName,
                                       100,
                                       docs)

      } else {
        logger.error(
          s"Error creating solr index: $index with this index name $indexName")
        throw new Exception("Error creating solr index " + index.name)
        //TODO handle errors
      }
    } else {
      logger.warn(
        s"The index '$id' does not exits pay ATTENTION the spark stream won't start")
    }
  }

}

class SolrSparkStructuredStreamingWriter(indexBL: IndexBL,
                                         val ss: SparkSession,
                                         val id: String,
                                         solrAdminActor: ActorRef)
    extends SparkStructuredStreamingWriter
    with SolrConfiguration
    with Logging {

  override def write(stream: DataFrame, queryName: String, checkpointDir: String): Unit = {

    // get index model from BL
    val indexOpt: Option[IndexModel] = indexBL.getById(id)
    if (indexOpt.isDefined) {
      val index = indexOpt.get

      // create streaming options
      val extraOptions = Map(
        "collection" -> indexOpt.get.collection,
        "zkhost" -> solrConfig.connections.mkString(","),
        // Generate unique key if the 'id' field does not exist
        "gen_uniq_key" -> "true"
      )

      // configure and start streaming
//      stream.writeStream
//        .format("solr")
//        .options(extraOptions)
//        .option("checkpointLocation", checkpointDir)
//        .queryName(queryName)
//        .start()

      val solrWriter = new SolrForeatchWriter(ss, index)

      stream
        .writeStream
        .options(extraOptions)
        .option("checkpointLocation", checkpointDir)
        .foreach(solrWriter)
        .queryName(queryName)
        .start()

    } else {
      logger.warn(
        s"The index '$id' does not exits pay ATTENTION the spark stream won't start")
    }

  }

}

class SolrForeatchWriter(val ss: SparkSession, val indexModel: IndexModel) extends ForeachWriter[Row]
  with SolrConfiguration
  with Logging {
  override def open(partitionId: Long, version: Long): Boolean = true

  override def process(value: Row): Unit = {
    val indexName = ConfigManager.buildTimedName(indexModel.name)

    val docs = SolrSparkWriter.createSolrDocument(value)
//    SolrSupport.indexDStreamOfDocs(solrConfig.connections.mkString(","),
//      indexName,
//      100,
//      docs)

    val a: RDD[SolrInputDocument] = ss.sparkContext.parallelize(Seq(docs))

    SolrSupport.indexDocs(solrConfig.connections.mkString(","),
      indexName,
      1000,
      a)
  }

  override def close(errorOrNull: Throwable): Unit = {}
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
      val indexName = ConfigManager.buildTimedName(index.name)

      logger.info(
        s"Check or create the index model: '${index.toString} with this index name: $indexName")

      if (??[Boolean](
            solrAdminActor,
            CheckOrCreateCollection(
              indexName,
              index.schema.get.get("properties").asArray().toString,
              index.numShards.getOrElse(1),
              index.replicationFactor.getOrElse(1))
          )) {

        val docs = data.rdd.map { r =>
          SolrSparkWriter.createSolrDocument(r)
        }

        SolrSupport.indexDocs(solrConfig.connections.mkString(","),
                              indexName,
                              100,
                              docs)

      } else {
        throw new Exception("Error creating solr index " + index.name)
        //TODO handle errors
      }

    } else {
      logger.warn(
        s"The index '$id' does not exits pay ATTENTION spark won't start")

    }

  }
}
