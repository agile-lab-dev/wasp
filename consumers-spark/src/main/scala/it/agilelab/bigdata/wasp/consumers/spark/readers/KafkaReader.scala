package it.agilelab.bigdata.wasp.consumers.spark.readers

import it.agilelab.bigdata.wasp.consumers.spark.readers.KafkaReader.logger
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.WaspSystem.??
import it.agilelab.bigdata.wasp.core.kafka.CheckOrCreateTopic
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.TopicModel
import it.agilelab.bigdata.wasp.core.utils.{AvroToJsonUtil, ConfigManager, JsonToByteArrayUtil}
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils

// TODO mock
object KafkaStructuredReader extends StructuredStreamingReader with Logging {

  /**
    *
    * Create a Dataframe from a streaming source
    *
    * @param group
    * @param accessType
    * @param topic
    * @param ss
    * @return
    */
  override def createStructuredStream(
      group: String,
      accessType: String,
      topic: TopicModel)(implicit ss: SparkSession): DataFrame = {

    // get the config
    val kafkaConfig = ConfigManager.getKafkaConfig

    // check or create
    if (??[Boolean](
          WaspSystem.kafkaAdminActor,
          CheckOrCreateTopic(topic.name, topic.partitions, topic.replicas))) {

      // create the stream
      val receiver = ss.readStream
        .format("kafka")
        .option("subscribe", topic.name)
        .option("kafka.bootstrap.servers", kafkaConfig.zookeeper.toString)
        .option("kafkaConsumer.pollTimeoutMs", kafkaConfig.ingestRateToMills())
        .load()
        // retrive key and values
//        .selectExpr("topic", "key", "value")

      // prepare the udf
      val avroToJson: Array[Byte] => String = AvroToJsonUtil.avroToJson
      val byteArrayToJson: Array[Byte] => String = JsonToByteArrayUtil.byteArrayToJson

      import org.apache.spark.sql.functions.udf
      val avroToJsonUDF = udf(avroToJson)
      val byteArrayToJsonUDF = udf(byteArrayToJson)

      topic.topicDataType match {
        case "avro" => receiver.withColumn("value", avroToJsonUDF())
        case "json" => receiver.withColumn("value", byteArrayToJsonUDF())
        case _ => receiver.withColumn("value", avroToJsonUDF())
      }

    } else {
      logger.error(s"Topic not found on Kafka: $topic")
      throw new Exception(s"Topic not found on Kafka: $topic")
    }
  }
}

object KafkaReader extends StreamingReader with Logging {

  /**
    * Kafka configuration
    */
  //TODO: check warning (not understood)
  def createStream(group: String, accessType: String, topic: TopicModel)(
      implicit ssc: StreamingContext): DStream[String] = {
    val kafkaConfig = ConfigManager.getKafkaConfig

    val kafkaConfigMap: Map[String, String] = Map(
      "zookeeper.connect" -> kafkaConfig.zookeeper.toString,
      "zookeeper.connection.timeout.ms" -> kafkaConfig.zookeeper.timeout
        .getOrElse(ConfigManager.getWaspConfig.servicesTimeoutMillis)
        .toString
    )

    if (??[Boolean](
          WaspSystem.kafkaAdminActor,
          CheckOrCreateTopic(topic.name, topic.partitions, topic.replicas))) {

      val receiver: DStream[(String, Array[Byte])] = accessType match {
        case "direct" =>
          KafkaUtils.createDirectStream[String,
                                        Array[Byte],
                                        StringDecoder,
                                        DefaultDecoder](
            ssc,
            kafkaConfigMap + ("group.id" -> group) + ("metadata.broker.list" -> kafkaConfig.connections
              .mkString(",")),
            Set(topic.name)
          )
        case "receiver-based" | _ =>
          KafkaUtils
            .createStream[String, Array[Byte], StringDecoder, DefaultDecoder](
              ssc,
              kafkaConfigMap + ("group.id" -> group),
              Map(topic.name -> 3),
              StorageLevel.MEMORY_AND_DISK_2
            )
      }

      topic.topicDataType match {
        case "avro" =>
          receiver.map(x => (x._1, AvroToJsonUtil.avroToJson(x._2))).map(_._2)
        case "json" =>
          receiver
            .map(x => (x._1, JsonToByteArrayUtil.byteArrayToJson(x._2)))
            .map(_._2)
        case _ =>
          receiver.map(x => (x._1, AvroToJsonUtil.avroToJson(x._2))).map(_._2)
      }

    } else {
      logger.error(s"Topic not found on Kafka: $topic")
      throw new Exception(s"Topic not found on Kafka: $topic")
    }
  }
}
