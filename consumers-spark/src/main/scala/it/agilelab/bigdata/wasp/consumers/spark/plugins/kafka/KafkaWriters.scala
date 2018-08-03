package it.agilelab.bigdata.wasp.consumers.spark.plugins.kafka

import java.util.UUID

import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkLegacyStreamingWriter, SparkStructuredStreamingWriter}
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.WaspSystem._
import it.agilelab.bigdata.wasp.core.bl.TopicBL
import it.agilelab.bigdata.wasp.core.kafka.{CheckOrCreateTopic, WaspKafkaWriter}
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.TopicModel
import it.agilelab.bigdata.wasp.core.models.configuration.{KafkaEntryConfig, TinyKafkaConfig}
import it.agilelab.bigdata.wasp.core.utils.{AvroToJsonUtil, ConfigManager, StringToByteArrayUtil, RowToAvro}
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

class KafkaSparkLegacyStreamingWriter(topicBL: TopicBL,
                                      ssc: StreamingContext,
                                      name: String)
  extends SparkLegacyStreamingWriter {

  override def write(stream: DStream[String]): Unit = {

    val kafkaConfig = ConfigManager.getKafkaConfig
    val tinyKafkaConfig = kafkaConfig.toTinyConfig()

    val topicOpt: Option[TopicModel] = topicBL.getByName(name)
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
                case "json" => StringToByteArrayUtil.stringToByteArray(record)
                case "avro" => AvroToJsonUtil.jsonToAvro(record, schemaB.value)
                case _ => AvroToJsonUtil.jsonToAvro(record, schemaB.value)
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

class KafkaSparkStructuredStreamingWriter(topicBL: TopicBL,
                                          name: String,
                                          ss: SparkSession)
  extends SparkStructuredStreamingWriter
    with Logging {

  override def write(stream: DataFrame,
                     queryName: String,
                     checkpointDir: String): StreamingQuery = {

    import ss.implicits._

    val kafkaConfig = ConfigManager.getKafkaConfig
    val tinyKafkaConfig = kafkaConfig.toTinyConfig()

    val topicOpt: Option[TopicModel] = topicBL.getByName(name)


    if (topicOpt.isDefined) {

      val topic = topicOpt.get
      val topicDataTypeB = ss.sparkContext.broadcast(topic.topicDataType)

      if (??[Boolean](WaspSystem.kafkaAdminActor, CheckOrCreateTopic(topic.name, topic.partitions, topic.replicas))) {

        logger.debug(s"Schema DF spark, topic name ${topic.name}:\n${stream.schema.treeString}")

        val pkf = topic.partitionKeyField
        //val pkfIndex: Option[Int] = pkf.map(k => stream.schema.fieldIndex(k))
        val dswParsed = topicDataTypeB.value match {
          case "avro" => {
            val converter: RowToAvro = RowToAvro(stream.schema, topic.name, "wasp", None, Some(topic.getJsonSchema))
            logger.debug(s"Schema Avro, topic name ${topic.name}:\n${converter.getSchema().toString(true)}")

            if (pkf.isDefined) {
              /* create a temp key (its name is not really relevant but have to be unique)
                  e.g. pkf = "aaa.bbb" => key = "key_aaa_bbb__<UUID>" (UUID "underscored" instead "dotted")

                  N.B. pkf have to be related to a String field
               */
              val tempKey = s"key_${pkf.get}__${UUID.randomUUID().toString}".replaceAll("[\\.-]", "_")
              logger.debug(s"tempKey: $tempKey")

              val streamWithKey = stream.selectExpr(s"${pkf.get} AS $tempKey", "*")
              logger.debug(s"SchemaWithKey DF spark, topic name ${topic.name}:\n${streamWithKey.schema.treeString}")

              streamWithKey.map(r => {
                val keyAndData = r.toSeq
                val key: String = keyAndData.head.asInstanceOf[String]  // have to be string
                val dataRow = Row.fromSeq(keyAndData.tail)

                (key, converter.write(dataRow))
              }).toDF("key", "value")
            }
            else {
              stream.map(r => {
                val key: String = null

                (key, converter.write(r))
              }).toDF("key", "value")
            }
          }
          case _ => {
            // json conversion
            if (pkf.isDefined) {
              val streamWithKey = stream.selectExpr(s"${pkf.get} AS key", "to_json(struct(*)) AS value")
              logger.debug(s"SchemaWithKey DF spark, topic name ${topic.name}:\n${streamWithKey.schema.treeString}")

              streamWithKey
            }
            else
              stream.selectExpr("to_json(struct(*)) AS value")
          }
        }

        val partialStreamWriter = dswParsed
          .writeStream
          .format("kafka")
          .option("topic", topic.name)
          .option("checkpointLocation", checkpointDir)

        val streamWriter =
          if(ConfigManager.getSparkStreamingConfig.triggerIntervalMs.isDefined)
            partialStreamWriter
              .trigger(Trigger.ProcessingTime(ConfigManager.getSparkStreamingConfig.triggerIntervalMs.get))
              .queryName(queryName)
          else
            partialStreamWriter.queryName(queryName)

        val dswWithWritingConf = addKafkaConf(streamWriter, tinyKafkaConfig)

        dswWithWritingConf.start()
      } else {
        val msg = s"Error creating topic ${topic.name}"
        throw new Exception(msg)
      }
    } else {
      val msg = s"No Topic specified in writer model"
      throw new Exception(msg)
    }

  }

  private def addKafkaConf(dsw: DataStreamWriter[Row], tkc: TinyKafkaConfig): DataStreamWriter[Row] = {

    val connectionString = tkc.connections.map{
      conn => s"${conn.host}:${conn.port}"
    }.mkString(",")

    val kafkaConfigMap: Seq[KafkaEntryConfig] = tkc.others

    dsw
      .option("kafka.bootstrap.servers", connectionString)
      .option("value.serializer", tkc.default_encoder)
      .option("key.serializer", tkc.encoder_fqcn)
      .option("kafka.partitioner.class", tkc.partitioner_fqcn)
      .option("kafka.batch.size", tkc.batch_send_size.toString)
      .option("kafka.acks", tkc.acks)
      .options(kafkaConfigMap.map(_.toTupla).toMap)
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