package it.agilelab.bigdata.wasp.consumers.spark.readers

import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.WaspSystem._
import it.agilelab.bigdata.wasp.core.kafka.CheckOrCreateTopic
import it.agilelab.bigdata.wasp.core.logging.WaspLogger
import it.agilelab.bigdata.wasp.core.models.{DefaultConfiguration, TopicModel}
import it.agilelab.bigdata.wasp.core.utils.{AvroToJsonUtil, ConfigManager, JsonToByteArrayUtil}
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
 * Created by Mattia Bertorello on 05/10/15.
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
object KafkaReader extends StreamingReader {
  val logger = WaspLogger(this.getClass.getName)


  /**
   * Kafka configuration
   */

  //TODO: check warning (not understood)
  def createStream(group: String, accessType: String, topic: TopicModel)(implicit ssc: StreamingContext): DStream[String] = {
    val kafkaConfig = ConfigManager.getKafkaConfig

    val kafkaConfigMap: Map[String, String] = Map(
      "zookeeper.connect" -> kafkaConfig.zookeeper.toString,
      "zookeeper.connection.timeout.ms" -> kafkaConfig.zookeeper.timeout.getOrElse(DefaultConfiguration.timeout).toString
    )


    if (??[Boolean](WaspSystem.getKafkaAdminActor, CheckOrCreateTopic(topic.name, topic.partitions, topic.replicas))) {

      val receiver: DStream[(String, Array[Byte])] =  accessType match {
        case "direct" => KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](
          ssc,
          kafkaConfigMap + ("group.id" -> group) + ("metadata.broker.list" -> kafkaConfig.connections.mkString(",")),
          Set(topic.name)
        )
        case "receiver-based" | _ => KafkaUtils.createStream[String, Array[Byte], StringDecoder, DefaultDecoder](
          ssc,
          kafkaConfigMap + ("group.id" -> group),
          Map(topic.name -> 3),
          StorageLevel.MEMORY_AND_DISK_2
        )
      }

      topic.topicDataType match {
        case "avro" => receiver.map(x => (x._1, AvroToJsonUtil.avroToJson(x._2))).map(_._2)
        case "json" => receiver.map(x => (x._1, JsonToByteArrayUtil.byteArrayToJson(x._2))).map(_._2)
        case _ => receiver.map(x => (x._1, AvroToJsonUtil.avroToJson(x._2))).map(_._2)
      }

    } else {
      logger.error(s"Topic not found on Kafka: $topic")
      throw new Exception(s"Topic not found on Kafka: $topic")
    }
  }
}
