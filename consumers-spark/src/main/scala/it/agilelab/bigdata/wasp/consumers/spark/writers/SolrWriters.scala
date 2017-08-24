package it.agilelab.bigdata.wasp.consumers.spark.writers

import com.lucidworks.spark.SolrSupport
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.WaspSystem._
import it.agilelab.bigdata.wasp.core.bl.IndexBL
import it.agilelab.bigdata.wasp.core.models.IndexModel
import it.agilelab.bigdata.wasp.core.solr.CheckOrCreateCollection
import it.agilelab.bigdata.wasp.core.utils.{ConfigManager, SolrConfiguration}
import org.apache.solr.common.SolrInputDocument
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.JavaDStream
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by matbovet on 02/09/2016.
  */
object SolrSparkWriter {

  def createSolrDocument(r: Row) = {
    val doc: SolrInputDocument = new SolrInputDocument()
    doc.setField("id", java.util.UUID.randomUUID.toString())

    val fieldname = r.schema.fieldNames
    fieldname.map { f =>
      doc.setField(f, r.getAs(f))
    }

    doc
  }

}

class SolrSparkStreamingWriter(env: { val indexBL: IndexBL }, val ssc: StreamingContext, val id: String)
    extends SparkStreamingWriter
    with SolrConfiguration {

  override def write(stream: DStream[String]): Unit = {

    val sqlContext = SQLContext.getOrCreate(ssc.sparkContext)
    val indexOpt: Option[IndexModel] = env.indexBL.getById(id)
    indexOpt.foreach(index => {

      val indexName = ConfigManager.buildTimedName(index.name)

      println(s"${index.toString()}")

      if (??[Boolean](
              WaspSystem.solrAdminActor,
              CheckOrCreateCollection(
                  indexName,
                  index.schema.get.get("properties").asArray().toString,
                  index.numShards.getOrElse(1),
                  index.replicationFactor.getOrElse(1)))) {

        val docs: DStream[SolrInputDocument] = stream.transform { rdd =>
          val df: Dataset[Row] = sqlContext.read.json(rdd)

          df.rdd.map { r =>
            SolrSparkWriter.createSolrDocument(r)
          }
        }

        SolrSupport.indexDStreamOfDocs(
            solrConfig.connections.mkString(","),
            indexName,
            100,
            new JavaDStream[SolrInputDocument](docs))

      } else {
        throw new Exception("Error creating solr index " + index.name)
        //TODO handle errors
      }

    })
  }

}

class SolrSparkWriter(env: { val indexBL: IndexBL }, sc: SparkContext, id: String)
    extends SparkWriter
    with SolrConfiguration {

  override def write(data: DataFrame): Unit = {

    val sqlContext = SQLContext.getOrCreate(sc)
    val indexOpt = env.indexBL.getById(id)
    indexOpt.foreach(index => {

      val indexName = ConfigManager.buildTimedName(index.name)

      if (??[Boolean](
              WaspSystem.solrAdminActor,
              CheckOrCreateCollection(
                  indexName,
                  index.schema.get.get("properties").asArray().toString,
                  index.numShards.getOrElse(1),
                  index.replicationFactor.getOrElse(1)))) {

        val docs = data.rdd.map { r =>
          SolrSparkWriter.createSolrDocument(r)
        }

        SolrSupport.indexDocs(solrConfig.connections.mkString(","),
                              indexName,
                              100,
                              new JavaRDD[SolrInputDocument](docs))

      } else {
        throw new Exception("Error creating solr index " + index.name)
        //TODO handle errors
      }

    })

  }
}
