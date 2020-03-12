package it.agilelab.bigdata.wasp.consumers.spark.plugins.kafka

import it.agilelab.bigdata.wasp.consumers.spark.writers.SparkLegacyStreamingWriter
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.WaspSystem.??
import it.agilelab.bigdata.wasp.core.bl.TopicBL
import it.agilelab.bigdata.wasp.core.kafka.{CheckOrCreateTopic, WaspKafkaWriter}
import it.agilelab.bigdata.wasp.core.models.TopicModel
import it.agilelab.bigdata.wasp.core.utils.{AvroToJsonUtil, ConfigManager, StringToByteArrayUtil}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

class KafkaSparkLegacyStreamingWriter(topicBL: TopicBL,
                                      ssc: StreamingContext,
                                      name: String)
  extends SparkLegacyStreamingWriter {

  override def write(stream: DStream[String]): Unit = {

    val kafkaConfig = ConfigManager.getKafkaConfig
    val tinyKafkaConfig = kafkaConfig.toTinyConfig()

    val topicOpt: Option[TopicModel] = topicBL.getTopicModelByName(name)
    topicOpt.foreach(topic => {

      if (??[Boolean](WaspSystem.kafkaAdminActor, CheckOrCreateTopic(topic.name, topic.partitions, topic.replicas))) {

        val schemaB = ssc.sparkContext.broadcast(topic.getJsonSchema)
        val configB = ssc.sparkContext.broadcast(tinyKafkaConfig)
        val topicNameB = ssc.sparkContext.broadcast(topic.name)
        val topicDataTypeB = ssc.sparkContext.broadcast(topic.topicDataType)

        stream.foreachRDD(rdd => {
          rdd.foreachPartition(partitionOfRecords => {

            // TODO remove ???
            // val writer = WorkerKafkaWriter.writer(configB.value)

            val writer = new WaspKafkaWriter[String, Array[Byte]](configB.value)

            partitionOfRecords.foreach(record => {
              val bytes = topicDataTypeB.value match {
                case "json" | "plaintext" => StringToByteArrayUtil.stringToByteArray(record)
                case "avro" => AvroToJsonUtil.jsonToAvro(record, schemaB.value, topic.useAvroSchemaManager)
                case _ => AvroToJsonUtil.jsonToAvro(record, schemaB.value, topic.useAvroSchemaManager)
              }
              writer.send(topicNameB.value, null, bytes)

            })

            writer.close()
          })
        })

      } else {
        val msg = s"Error creating topic ${topic.name}"
        throw new Exception(msg)
      }
    })
  }
}

