package it.agilelab.bigdata.wasp.consumers.spark.plugins.kafka

import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkLegacyStreamingWriter, SparkStructuredStreamingWriter}
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.WaspSystem._
import it.agilelab.bigdata.wasp.core.bl.TopicBL
import it.agilelab.bigdata.wasp.core.kafka.{CheckOrCreateTopic, WaspKafkaWriter}
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.TopicModel
import it.agilelab.bigdata.wasp.core.models.configuration.TinyKafkaConfig
import it.agilelab.bigdata.wasp.core.utils.{AvroToJsonUtil, ConfigManager, JsonToByteArrayUtil, RowToAvro}
import org.apache.spark.sql.streaming.DataStreamWriter
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
                case "json" => JsonToByteArrayUtil.jsonToByteArray(record)
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
                     checkpointDir: String): Unit = {

    import ss.implicits._

    val kafkaConfig = ConfigManager.getKafkaConfig
    val tinyKafkaConfig = kafkaConfig.toTinyConfig()

    val topicOpt: Option[TopicModel] = topicBL.getByName(name)
    topicOpt.foreach(topic => {

      val topicDataTypeB = ss.sparkContext.broadcast(topic.topicDataType)

      if (??[Boolean](WaspSystem.kafkaAdminActor, CheckOrCreateTopic(topic.name, topic.partitions, topic.replicas))) {

//        val kafkaFormattedDF = stream.toJSON.map{
//          json =>
//            val payload = topicDataTypeB.value match {
//              case "avro" => AvroToJsonUtil.jsonToAvro(json, schemaB.value)
//              case "json" => JsonToByteArrayUtil.jsonToByteArray(json)
//              case _ => AvroToJsonUtil.jsonToAvro(json, schemaB.value)
//            }
//            payload
//        }

        // partition key
        val pkf = topic.partitionKeyField
        val pkfIndex: Option[Int] = pkf.map(k => stream.schema.fieldIndex(k))

        val dswParsed = topicDataTypeB.value match {
          case "avro" => {
            val converter: RowToAvro = RowToAvro(stream.schema, topic.name, "wasp", None, Some(topic.getJsonSchema))
            logger.debug(s"Schema DF spark, topic name ${topic.name}: " + stream.schema.treeString)
            logger.debug(s"Schema Avro, topic name ${topic.name}: " + converter.getSchema().toString(true))

            stream.map(r => {
              val key: String = pkfIndex.map(r.getString).orNull
              (key, converter.write(r))
            }).toDF("key", "value")
          }
          case _ => {
            // json conversion
            if (pkf.isDefined)  stream.selectExpr(pkf.get, "to_json(struct(*)) AS value")
            else stream.selectExpr("to_json(struct(*)) AS value")
          }
        }

        val dswParsedReady = dswParsed
          .writeStream
          .format("kafka")
          .option("topic", topic.name)
          .option("checkpointLocation", checkpointDir)
          .queryName(queryName)

        val dswWithWritingConf = addKafkaConf(dswParsedReady, tinyKafkaConfig)

        dswWithWritingConf.start()
      } else {
        val msg = s"Error creating topic ${topic.name}"
        throw new Exception(msg)
      }
    })
  }

  private def addKafkaConf(dsw: DataStreamWriter[Row], tkc: TinyKafkaConfig): DataStreamWriter[Row] = {

    val connectionString = tkc.connections.map{
      conn => s"${conn.host}:${conn.port}"
    }.mkString(",")

    val otherrConfigs = tkc.otherConfigs.map(_.toTupla).toMap
    val kafkaConfigMap: Map[String, String] = Map[String, String](
      // Added for backwards compatibility
      "acks" -> "1"
    ) ++
      tkc.otherConfigs.map(_.toTupla)

    dsw
      .option("kafka.bootstrap.servers", connectionString)
      .option("serializer.class", tkc.default_encoder)
      .option("key.serializer.class", tkc.encoder_fqcn)
      .option("partitioner.class", tkc.partitioner_fqcn)
      .option("batch.size", tkc.batch_send_size.toString)
      .options(kafkaConfigMap)
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