package it.agilelab.bigdata.wasp.consumers.spark.readers

import it.agilelab.bigdata.wasp.core.models.TopicModel
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream


/**
 * Created by Mattia Bertorello on 05/10/15.
 * Modified by Matteo Bovetti on 02/10/17.
 */
trait StreamingReader {
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

trait StructuredStreamingReader {
  /**
    *
    * Create a Dataframe from a streaming source
    * @param group
    * @param accessType
    * @param topic
    * @param ss
    * @return
    */
  def createStructuredStream(group: String, accessType: String, topic: TopicModel)(implicit ss: SparkSession): DataFrame
}