package it.agilelab.bigdata.wasp.consumers.utils
/*
import it.agilelab.bigdata.wasp.consumers.readers.StreamingReader
import it.agilelab.bigdata.wasp.consumers.writers.{SparkStreamingWriter, SparkWriter, SparkWriterFactory}
import it.agilelab.bigdata.wasp.core.bl.{IndexBL, KeyValueBL, RawBL, TopicBL}
import it.agilelab.bigdata.wasp.core.models.{TopicModel, WriterModel}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{StreamingContext, Time}
import org.json4s.jackson.Serialization

import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSONObject

/**
 * Created by Mattia Bertorello on 05/10/15.
 */
class SparkWriterTestWrapper extends SparkWriterFactory {

  val resultSparkStreamingWriter = new ListBuffer[String]

  override def createSparkWriterStreaming(env: {val topicBL: TopicBL; val indexBL: IndexBL; val rawBL: RawBL; val keyValueBL: KeyValueBL}, ssc: StreamingContext, writerModel: WriterModel): Option[SparkStreamingWriter] = {
    val writer = new SparkStreamingWriter {
      override def write(stream: DStream[String]): Unit = {
        stream.foreachRDD((rdd: RDD[String], time: Time) => {
          resultSparkStreamingWriter.++=(rdd.collect())
        })
      }
    }
    Some(writer)
  }


  val resultSparkWriter = new ListBuffer[Row]
  override def createSparkWriterBatch(env: {val indexBL: IndexBL; val rawBL: RawBL; val keyValueBL: KeyValueBL}, sc: SparkContext, writerModel: WriterModel): Option[SparkWriter] = {
    val writer = new SparkWriter {
      override def write(data: DataFrame): Unit = resultSparkWriter.++=(data.collect())
    }
    Some(writer)
  }
}

class StreamingReaderTestWrapper(sc: SparkContext) extends StreamingReader {
  val lines = scala.collection.mutable.Queue[RDD[String]]()

  /**
   *
   * Create a DStream from a streaming source
    *
    * @param group
   * @param topic
   * @param ssc Spark streaming context
   * @return a json encoded string
   */
  override def createStream(group: String, accessType: String, topic: TopicModel)(implicit ssc: StreamingContext): DStream[String] = {
    ssc.queueStream(lines)
  }

  def addRDDToStreaming(data: String): Unit  = addRDDToStreaming(Seq(data))


  def addRDDToStreaming(data: Map[String, Any]): Unit  = {
    import org.json4s._

    implicit val formats = Serialization.formats(NoTypeHints)
    import org.json4s.native.Serialization.write
    val t = write(data)

    addRDDToStreaming(t)
  }

  def addRDDToStreamingMap(data: Seq[Map[String, Any]]): Unit  = addRDDToStreaming(data.map(JSONObject(_).toString()))


  def addRDDToStreaming(data: Seq[String]): Unit = {
    // append data to DStream
    lines += sc.makeRDD(data)
  }

*/
}