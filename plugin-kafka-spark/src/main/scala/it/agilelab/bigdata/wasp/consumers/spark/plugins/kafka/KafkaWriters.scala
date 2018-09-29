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
import it.agilelab.bigdata.wasp.core.utils.{AvroToJsonUtil, ConfigManager, RowToAvro, StringToByteArrayUtil}
import it.agilelab.bigdata.wasp.spark.sql.kafka011.KafkaSparkSQLSchemas.HEADER_DATA_TYPE_NULL_VALUE
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.types._
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
                case "json" | "plaintext" => StringToByteArrayUtil.stringToByteArray(record)
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
  
  override def write(stream: DataFrame): DataStreamWriter[Row] = {

    import ss.implicits._

    val sqlContext = stream.sqlContext
    
    val kafkaConfig = ConfigManager.getKafkaConfig
    val tinyKafkaConfig = kafkaConfig.toTinyConfig()

    val topicOpt: Option[TopicModel] = topicBL.getByName(name)

    if (topicOpt.isDefined) {

      val topic = topicOpt.get
      
      logger.info(s"Writing with topic model: $topic")

      if (??[Boolean](WaspSystem.kafkaAdminActor, CheckOrCreateTopic(topic.name, topic.partitions, topic.replicas))) {

        logger.debug(s"Input schema:\n${stream.schema.treeString}")

        val keyFieldName = topic.keyFieldName
        val headersFieldName = topic.headersFieldName
        val valueFieldsNames = topic.valueFieldsNames
        
        def convertInputToKafkaMessage() = {
          // generate temporary field names
          val tempKeyFieldName = s"key_${UUID.randomUUID().toString}".replaceAll("[\\.-]", "_")
          val tempHeadersFieldName = s"headers_${UUID.randomUUID().toString}".replaceAll("[\\.-]", "_")
  
          // generate select expressions to clone metadata columns and keep only the values specified
          val selectExpressionsForTempColumns =
            keyFieldName.map(kfn => s"CAST($kfn AS binary) $tempKeyFieldName").toList ++
            headersFieldName.map(hfn => s"$hfn AS $tempHeadersFieldName").toList ++
            valueFieldsNames.map(vfn => vfn).getOrElse(Seq("*"))
          logger.debug(s"Generated select expressions: ${selectExpressionsForTempColumns.mkString("[", "], [", "]")}")
  
          // project the data so we have a known order for the metadata columns with the rest of the data after
          val streamWithTempColumns = stream.selectExpr(selectExpressionsForTempColumns: _*)
          logger.debug(s"Stream with temp columns schema:\n${streamWithTempColumns.schema.treeString}")
  
          // this tells us where the data starts, everything eventually present before is metadata
          val dataOffset = Seq(keyFieldName, headersFieldName).count(_.isDefined)
  
          // generate a schema and avro converter for the values
          val valueSchema = StructType(
            streamWithTempColumns.schema.drop(dataOffset)
          )
          logger.debug(s"Generated value schema:\n${valueSchema.treeString}")
          // TODO use sensible namespace instead of wasp
          val converter = RowToAvro(valueSchema, topic.name, "wasp", None, Some(topic.getJsonSchema))
          val dataConverter: Row => Array[Byte] = converter.write
  
          // generate a schema and encoder for final metadata & data
          val schema = StructType(
            keyFieldName.map(_ => StructField("key", BinaryType, nullable = true)).toList ++
              headersFieldName.map(_ => StructField("headers", HEADER_DATA_TYPE_NULL_VALUE, nullable = false)).toList :+
              StructField("value", BinaryType, nullable = false)
          )
          logger.debug(s"Generated final schema:\n${schema.treeString}")
          val encoder = RowEncoder(schema)
  
          // process the stream, extracting the data and converting it, and leaving metadata as is
          val processedStream = streamWithTempColumns.map(row => {
            val inputElements = row.toSeq
            val metadata = inputElements.take(dataOffset)
            val data = inputElements.drop(dataOffset)
            val convertedData = dataConverter(Row.fromSeq(data))
            val outputElements = metadata :+ convertedData
            Row.fromSeq(outputElements)
          })(encoder)
          
          processedStream
        }
        
        val finalStream = topic.topicDataType match {
          case "avro" => {
            // convert input
            convertInputToKafkaMessage()
          }
          case "json" => {
            // generate select expressions to rename matadata columns and convert everything to json
            val selectExpressions =
              keyFieldName.map(kfn => s"$kfn AS key").toList ++
              headersFieldName.map(hfn => s"$hfn AS headers").toList :+
              "to_json(struct(*)) AS value"
            
            // convert input
            stream.selectExpr(selectExpressions: _*)
          }
          case "plaintext" => // TODO this is broken
            if (keyFieldName.isDefined) {
              val streamWithKey = stream.selectExpr(s"${keyFieldName.get} AS key", "* AS value")
              logger.debug(s"SchemaWithKey DF spark, topic name ${topic.name}:\n${streamWithKey.schema.treeString}")

              streamWithKey
            }
            else
              stream.selectExpr("* AS value")
          case topicDataType => throw new UnsupportedOperationException(s"Unknown topic data type $topicDataType")
        }

        val partialStreamWriter = finalStream
          .writeStream
          .format("kafka")
          .option("topic", topic.name)

        val dswWithWritingConf = addKafkaConf(partialStreamWriter, tinyKafkaConfig)

        dswWithWritingConf
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