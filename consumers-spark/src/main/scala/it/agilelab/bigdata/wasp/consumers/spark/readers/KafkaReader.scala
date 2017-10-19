package it.agilelab.bigdata.wasp.consumers.spark.readers

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.WaspSystem.??
import it.agilelab.bigdata.wasp.core.kafka.CheckOrCreateTopic
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.TopicModel
import it.agilelab.bigdata.wasp.core.utils.AvroToJsonUtil.logger
import it.agilelab.bigdata.wasp.core.utils.{AvroToJsonUtil, ConfigManager, JsonToByteArrayUtil, SimpleUnionJsonEncoder}
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils

object KafkaStructuredReader extends StructuredStreamingReader with Logging {

  def avroToJsonInternal: Array[Byte] => String =
    (avro: Array[Byte]) => {
      logger.debug("Starting avroToJson encoding ...")

      val pretty = false
      val JsonEncoder = null

      val reader = new GenericDatumReader[GenericRecord]()
      val input = new ByteArrayInputStream(avro)
      val streamReader = new DataFileStream[GenericRecord](input, reader)
      val output = new ByteArrayOutputStream()

      val schema = streamReader.getSchema
      val writer = new GenericDatumWriter[GenericRecord](schema)
      val encoder = new SimpleUnionJsonEncoder(schema, output)

      while (streamReader.iterator.hasNext) {
        writer.write(streamReader.iterator().next(), encoder)
      }

      encoder.flush()
      output.flush()
      new String(output.toByteArray, "UTF-8")
  }

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

      logger.info("ss.readStream")
      // create the stream
      val r = ss.readStream
        .format("kafka")
        .option("subscribe", topic.name)
        .option("kafka.bootstrap.servers", kafkaConfig.zookeeper.toString)
        .option("kafkaConsumer.pollTimeoutMs", kafkaConfig.ingestRateToMills())
        .load()
        // retrive key and values

      import ss.implicits._
      val receiver = r.selectExpr("CAST(key AS STRING)", "CAST(value as STRING)").as[(String, String)]

      val q = receiver
        .writeStream
        .format("kafka")
        .option("topic", "testmatteo")
        .option("checkpointLocation", "/home/matteo/data/ckp")
        .start()

      q.awaitTermination()

      r

      // prepare the udf
//      val avroToJson: Array[Byte] => String = avroToJsonInternal
////      val byteArrayToJson: Array[Byte] => String = JsonToByteArrayUtil.byteArrayToJson
//
//      import org.apache.spark.sql.functions._
//      val avroToJsonUDF = udf(avroToJson)
////      val byteArrayToJsonUDF = udf(byteArrayToJson)
//
//
//      topic.topicDataType match {
//        case "avro" => receiver.withColumn("value2", avroToJsonUDF(col("value"))).withColumnRenamed("value2", "value")
////        case "json" => receiver.withColumn("value2", byteArrayToJsonUDF()).withColumnRenamed("value2", "value")
//        case _ => receiver.withColumn("value2", avroToJsonUDF()).withColumnRenamed("value2", "value")
//      }

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
