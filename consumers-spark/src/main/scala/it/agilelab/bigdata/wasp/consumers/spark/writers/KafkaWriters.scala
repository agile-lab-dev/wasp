package it.agilelab.bigdata.wasp.consumers.spark.writers

import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.WaspSystem._
import it.agilelab.bigdata.wasp.core.bl.TopicBL
import it.agilelab.bigdata.wasp.core.kafka.{CheckOrCreateTopic, WaspKafkaWriter}
import it.agilelab.bigdata.wasp.core.models.configuration.TinyKafkaConfig
import it.agilelab.bigdata.wasp.core.utils.{AvroToJsonUtil, ConfigManager, JsonToByteArrayUtil}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.sql.kafka010._
import org.apache.spark.sql.streaming.DataStreamWriter


class KafkaSparkStreamingWriter(env: {val topicBL: TopicBL}, ssc: StreamingContext, id: String)
  extends SparkStreamingWriter {

  override def write(stream: DStream[String]): Unit = {
    val kafkaConfig = ConfigManager.getKafkaConfig

    val topicOpt = env.topicBL.getById(id)
    topicOpt.foreach(topic => {

      if (??[Boolean](WaspSystem.kafkaAdminActor, CheckOrCreateTopic(topic.name, topic.partitions, topic.replicas))) {

        val schemaB = ssc.sparkContext.broadcast(topic.getJsonSchema)
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

class KafkaSparkStructuredStreamingWriter(env: {val topicBL: TopicBL}, id: String, ss: SparkSession)
  extends SparkStructuredStreamingWriter {
  override def write(stream: DataFrame): Unit = {
    val kafkaConfig = ConfigManager.getKafkaConfig

    val topicOpt = env.topicBL.getById(id)

    val tinyKafkaConfig = kafkaConfig.toTinyConfig()

    topicOpt.foreach(topic => {

      val topicDataTypeB = ss.sparkContext.broadcast(topic.topicDataType)
      val schemaB = ss.sparkContext.broadcast(topic.getJsonSchema)

      if (??[Boolean](WaspSystem.kafkaAdminActor, CheckOrCreateTopic(topic.name, topic.partitions, topic.replicas))) {

        val kafkaFormattedDF= stream.toJSON.map{
          json =>
            val payload = topicDataTypeB.value match {
              case "json" => JsonToByteArrayUtil.jsonToByteArray(json)
              case "avro" => AvroToJsonUtil.jsonToAvro(json, schemaB.value)
              case _ => AvroToJsonUtil.jsonToAvro(json, schemaB.value)
            }
            payload
        }


        //TODO partitionKey definition?
        val dsw: DataStreamWriter[Row] = kafkaFormattedDF
          .selectExpr("""'partitionKey'""", "value")
          .writeStream
          .format("kafka")
          .option("topic", topic.name)

        val dswWithWritingConf = addKafkaConf(dsw,tinyKafkaConfig)

        dswWithWritingConf.start()
      } else {
      throw new Exception("Error creating topic " + topic.name)
      //TODO handle errors
    }

    })
  }

  private def addKafkaConf(dsw: DataStreamWriter[Row], tkc: TinyKafkaConfig): DataStreamWriter[Row] = {

    val connectionString = tkc.connections.map{
      conn => s"${conn.host}:${conn.port}"
    }.mkString(",")

    dsw
      .option("kafka.bootstrap.servers", connectionString)
      .option("serializer.class", tkc.default_encoder)
      .option("key.serializer.class", tkc.encoder_fqcn)
      .option("partitioner.class", tkc.partitioner_fqcn)
      .option("producer.type", "async")
      .option("request.required.acks", "1")
      .option("batch.num.messages", tkc.batch_send_size.toString)
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