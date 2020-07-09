package it.agilelab.bigdata.wasp.consumers.spark.readers

import it.agilelab.bigdata.wasp.models.{StreamingReaderModel, StructuredStreamingETLModel, TopicModel}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by Mattia Bertorello on 05/10/15.
  * Modified by Matteo Bovetti on 02/10/17.
  */
trait SparkLegacyStreamingReader {
  /**
    *
    * Create a DStream from a streaming source
    * @param group
    * @param topic
    * @param ssc Spark streaming context
    * @return a json encoded string
    */
  def createStream(group: String, accessType: String, topic: TopicModel)(implicit ssc: StreamingContext): DStream[String]
  
}

trait SparkStructuredStreamingReader {
  /**
    * Create a streaming DataFrame from a streaming source.
    *
    * @param group the group of the ETL for which the stream is being created
    * @param streamingReaderModel the model for the streamign source from which the stream originates
    * @param ss the Spark Session to use
    * @return
    */
  def createStructuredStream(etl: StructuredStreamingETLModel, streamingReaderModel: StreamingReaderModel)(implicit ss: SparkSession): DataFrame
}

/**
  * Created by Mattia Bertorello on 10/09/15.
  */
trait SparkBatchReader {
  val name: String
  val readerType: String

  def read(sc: SparkContext): DataFrame
}