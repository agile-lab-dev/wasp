package it.agilelab.bigdata.wasp.consumers.spark.readers

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import it.agilelab.bigdata.wasp.core.{RawTopic, WaspSystem}
import it.agilelab.bigdata.wasp.core.WaspSystem.??
import it.agilelab.bigdata.wasp.core.kafka.CheckOrCreateTopic
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.TopicModel
import it.agilelab.bigdata.wasp.core.utils.AvroToJsonUtil.logger
import it.agilelab.bigdata.wasp.core.utils.{AvroToJsonUtil, ConfigManager, JsonToByteArrayUtil, SimpleUnionJsonEncoder}
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.avro.Schema
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils

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
    val schemaB = ss.sparkContext.broadcast(topic.getDataType)

    // check or create
    if (??[Boolean](
          WaspSystem.kafkaAdminActor,
          CheckOrCreateTopic(topic.name, topic.partitions, topic.replicas))) {

      import ss.implicits._

      logger.info("ss.readStream")
      // create the stream
      val r: DataFrame = ss.readStream
        .format("kafka")
        .option("subscribe", topic.name)
        .option("kafka.bootstrap.servers", kafkaConfig.connections.map(_.toString).mkString(","))
        .option("kafkaConsumer.pollTimeoutMs", kafkaConfig.ingestRateToMills())
        .load()

      // prepare the udf
      val avroToJson: Array[Byte] => String = AvroToJsonUtil.avroToJson
      val byteArrayToJson: Array[Byte] => String = JsonToByteArrayUtil.byteArrayToJson

      import org.apache.spark.sql.functions._
      val avroToJsonUDF = udf(avroToJson)
      val byteArrayToJsonUDF = udf(byteArrayToJson)

      val ret = topic.topicDataType match {
        case "avro" => r.withColumn("value_parsed", avroToJsonUDF(col("value")))
        case "json" => r.withColumn("value_parsed", byteArrayToJsonUDF(col("value")))
        case _ => r.withColumn("value_parsed", avroToJsonUDF(col("value")))
      }

      ret
        .drop("value")
        .select(from_json(col("value_parsed"), schemaB.value).alias("value"))
        .select(col("value.*"))

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
