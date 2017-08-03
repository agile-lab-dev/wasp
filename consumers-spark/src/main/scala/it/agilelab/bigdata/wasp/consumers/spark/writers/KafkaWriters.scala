package it.agilelab.bigdata.wasp.consumers.spark.writers

import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.WaspSystem._
import it.agilelab.bigdata.wasp.core.bl.TopicBL
import it.agilelab.bigdata.wasp.core.kafka.{CheckOrCreateTopic, WaspKafkaWriter}
import it.agilelab.bigdata.wasp.core.models.configuration.TinyKafkaConfig
import it.agilelab.bigdata.wasp.core.utils.{AvroToJsonUtil, BSONFormats, ConfigManager, JsonToByteArrayUtil}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import reactivemongo.bson.BSONDocument

import scala.concurrent.Await


class KafkaSparkStreamingWriter(env: {val topicBL: TopicBL}, ssc: StreamingContext, id: String)
  extends SparkStreamingWriter {

  override def write(stream: DStream[String]): Unit = {
    val kafkaConfig = ConfigManager.getKafkaConfig

    val topicFut = env.topicBL.getById(id)
    val topicOpt = Await.result(topicFut, timeout.duration)
    topicOpt.foreach(topic => {

      if (??[Boolean](WaspSystem.getKafkaAdminActor, CheckOrCreateTopic(topic.name, topic.partitions, topic.replicas))) {

        val schemaB = ssc.sparkContext.broadcast(BSONFormats.toString(topic.schema.getOrElse(BSONDocument())))
        val configB = ssc.sparkContext.broadcast(ConfigManager.getKafkaConfig.toTinyConfig())
        val topicNameB = ssc.sparkContext.broadcast(topic.name)
	      val topicDataTypeB = ssc.sparkContext.broadcast(topic.topicDataType)

        stream.foreachRDD(rdd => {
          rdd.foreachPartition(partitionOfRecords => {

            // TODO remove ???
            // val writer = WorkerKafkaWriter.writer(configB.value)

            val writer = new WaspKafkaWriter[String, Array[Byte]](configB.value)

            partitionOfRecords.foreach(record => {
              val bytes = topicDataTypeB.value match {
                case "json" => JsonToByteArrayUtil.jsonToByteArray(record)
                case "avro" => AvroToJsonUtil.jsonToAvro(record, schemaB.value)
                case _ => AvroToJsonUtil.jsonToAvro(record, schemaB.value)
              }
              writer.send(topicNameB.value, "partitionKey", bytes)

            })

            writer.close()
          })
        })

      } else {
        throw new Exception("Error creating topic " + topic.name)
        //TODO handle errors
      }
    })
  }
}

object WorkerKafkaWriter {
	//lazy producer creation allows to create a kafka conection per worker instead of per partition
	def writer(config: TinyKafkaConfig): WaspKafkaWriter[String, Array[Byte]] = {
		ProducerObject.config = config
		//thread safe
		ProducerObject.writer
	}
	
	object ProducerObject {
		var config: TinyKafkaConfig = _
    // TODO unused!
		lazy val writer = new WaspKafkaWriter[String, Array[Byte]](config)
	}
	
}