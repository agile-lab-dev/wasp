package it.agilelab.bigdata.wasp.consumers.spark.plugins.kafka

import java.util.UUID

import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkLegacyStreamingWriter, SparkStructuredStreamingWriter}
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.WaspSystem._
import it.agilelab.bigdata.wasp.core.bl.TopicBL
import it.agilelab.bigdata.wasp.core.datastores.TopicCategory
import it.agilelab.bigdata.wasp.core.kafka.{CheckOrCreateTopic, WaspKafkaWriter}
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.{DatastoreModel, MultiTopicModel, TopicModel}
import it.agilelab.bigdata.wasp.core.models.configuration.{KafkaEntryConfig, TinyKafkaConfig}
import it.agilelab.bigdata.wasp.core.utils.{AvroToJsonUtil, ConfigManager, RowToAvro, StringToByteArrayUtil}
import it.agilelab.bigdata.wasp.spark.sql.kafka011.KafkaSparkSQLSchemas.HEADER_DATA_TYPE_NULL_VALUE
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{col, udf}
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
                                          topicDatastoreModelName: String,
                                          ss: SparkSession)
  extends SparkStructuredStreamingWriter
    with Logging {
  
  override def write(stream: DataFrame): DataStreamWriter[Row] = {
    
    val sqlContext = stream.sqlContext
    
    val kafkaConfig = ConfigManager.getKafkaConfig
    val tinyKafkaConfig = kafkaConfig.toTinyConfig()

    val topicOpt: Option[DatastoreModel[TopicCategory]] = topicBL.getByName(topicDatastoreModelName)
    val (topicFieldName, topics) = topicOpt match {
      case Some(topicModel: TopicModel) => (None, Seq(topicModel))
      case Some(multiTopicModel: MultiTopicModel) =>
        val topics = multiTopicModel.topicModelNames.map(topicBL.getByName).flatMap({
          case Some(topicModel: TopicModel) =>
            Seq(topicModel)
          case None =>
            throw new Exception(s"""Unable to retrieve topic datastore model with name "$topicDatastoreModelName"""")
        })
        (Some(multiTopicModel.topicNameField), topics)
      case None =>
        throw new Exception(s"""Unable to retrieve topic datastore model with name "$topicDatastoreModelName"""")
    }
    val mainTopicModel = topicOpt.get
    val prototypeTopicModel = topics.head
    
    MultiTopicModel.validateTopicModels(topics)
  
    logger.info(s"Writing with topic model: $mainTopicModel")
    if (mainTopicModel.isInstanceOf[MultiTopicModel]) {
      logger.info(s"""Topic model "${mainTopicModel.name}" is a MultiTopicModel for topics: $topics""")
    }
    
    logger.info(s"Creating topics $topics")
    
    topics.foreach(topic =>
      if (! ??[Boolean](WaspSystem.kafkaAdminActor,
                      CheckOrCreateTopic(topic.name,
                                         topic.partitions,
                                         topic.replicas)))
        throw new Exception(s"""Error creating topic "${prototypeTopicModel.name}"""")
    )

    logger.debug(s"Input schema:\n${stream.schema.treeString}")

    val keyFieldName = prototypeTopicModel.keyFieldName
    val headersFieldName = prototypeTopicModel.headersFieldName
    val valueFieldsNames = prototypeTopicModel.valueFieldsNames
    
    val convertedStream = prototypeTopicModel.topicDataType match {
      case "avro" => {
        convertStreamForAvro(keyFieldName,
                             headersFieldName,
                             topicFieldName,
                             valueFieldsNames,
                             stream,
                             prototypeTopicModel)
      }
      case "json" => {
        convertStreamForJson(keyFieldName,
                             headersFieldName,
                             topicFieldName,
                             valueFieldsNames,
                             stream,
                             prototypeTopicModel)
      }
      case "plaintext" => {
        convertStreamForPlaintext(keyFieldName,
                                  headersFieldName,
                                  topicFieldName,
                                  valueFieldsNames,
                                  stream,
                                  prototypeTopicModel)
      }
      case "binary" => {
        convertStreamForBinary(keyFieldName,
                               headersFieldName,
                               topicFieldName,
                               valueFieldsNames,
                               stream,
                               prototypeTopicModel)
      }
      case topicDataType => throw new UnsupportedOperationException(s"Unknown topic data type $topicDataType")
    }
    
    val finalStream = addTopicNameCheckIfNeeded(topicFieldName, topics, convertedStream)

    val partialDataStreamWriter = finalStream
      .writeStream
      .format("kafka")
    
    val partialDataStreamWriterAfterTopicConf =
      if (topicFieldName.isDefined)
        partialDataStreamWriter
      else
        partialDataStreamWriter.option("topic", prototypeTopicModel.name)

    val finalDataStreamWriter = addKafkaConf(partialDataStreamWriterAfterTopicConf, tinyKafkaConfig)

    finalDataStreamWriter
  }
  
  private def convertStreamForAvro(keyFieldName: Option[String],
                                   headersFieldName: Option[String],
                                   topicFieldName: Option[String],
                                   valueFieldsNames: Option[Seq[String]],
                                   stream: DataFrame,
                                   prototypeTopicModel: TopicModel) = {
    // generate temporary field names
    val tempKeyFieldName = s"key_${UUID.randomUUID().toString}".replaceAll("[\\.-]", "_")
    val tempHeadersFieldName = s"headers_${UUID.randomUUID().toString}".replaceAll("[\\.-]", "_")
    val tempTopicFieldName = s"topic_${UUID.randomUUID().toString}".replaceAll("[\\.-]", "_")
  
    // generate select expressions to clone metadata columns and keep only the values specified
    val selectExpressionsForTempColumns =
      keyFieldName.map(kfn => s"CAST($kfn AS binary) $tempKeyFieldName").toList ++
        headersFieldName.map(hfn => s"$hfn AS $tempHeadersFieldName").toList ++
        topicFieldName.map(tfn => s"$tfn AS $tempTopicFieldName").toList ++
        valueFieldsNames.map(vfn => vfn).getOrElse(Seq("*"))
    logger.debug(s"Generated select expressions: ${selectExpressionsForTempColumns.mkString("[", "], [", "]")}")
  
    // project the data so we have a known order for the metadata columns with the rest of the data after
    val streamWithTempColumns = stream.selectExpr(selectExpressionsForTempColumns: _*)
    logger.debug(s"Stream with temp columns schema:\n${streamWithTempColumns.schema.treeString}")
  
    // this tells us where the data starts, everything eventually present before is metadata
    val dataOffset = Seq(keyFieldName, headersFieldName, topicFieldName).count(_.isDefined)
  
    // generate a schema and avro converter for the values
    val valueSchema = StructType(
      streamWithTempColumns.schema.drop(dataOffset)
    )
    logger.debug(s"Generated value schema:\n${valueSchema.treeString}")
    // TODO use sensible namespace instead of wasp
    val converter = RowToAvro(valueSchema, prototypeTopicModel.name, "wasp", None, Some(prototypeTopicModel.getJsonSchema))
    val dataConverter: Row => Array[Byte] = converter.write
  
    // generate a schema and encoder for final metadata & data
    val schema = StructType(
      keyFieldName.map(_ => StructField("key", BinaryType, nullable = true)).toList ++
        headersFieldName.map(_ => StructField("headers", HEADER_DATA_TYPE_NULL_VALUE, nullable = false)).toList ++
        topicFieldName.map(_ => StructField("topic", StringType, nullable = false)).toList:+
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
  
    logger.debug(s"Actual final schema:\n${processedStream.schema.treeString}")
  
    processedStream
  }
  
  private def convertStreamForJson(keyFieldName: Option[String],
                                   headersFieldName: Option[String],
                                   topicFieldName: Option[String],
                                   valueFieldsNames: Option[Seq[String]],
                                   stream: DataFrame,
                                   prototypeTopicModel: TopicModel) = {
    // generate select expressions to rename matadata columns and convert everything to json
    val valueSelectExpression = valueFieldsNames.map(vfn => vfn).getOrElse(Seq("*")).mkString(", ")
    val selectExpressions =
      keyFieldName.map(kfn => s"CAST($kfn AS binary) key").toList ++
        headersFieldName.map(hfn => s"$hfn AS headers").toList ++
        topicFieldName.map(tfn => s"$tfn AS topic").toList :+
        s"to_json(struct($valueSelectExpression)) AS value"
  
    logger.debug(s"Generated select expressions: ${selectExpressions.mkString("[", "], [", "]")}")
  
    // TODO check that json produced matches schema
    
    // convert input
    stream.selectExpr(selectExpressions: _*)
  }
  
  private def convertStreamForPlaintext(keyFieldName: Option[String],
                                        headersFieldName: Option[String],
                                        topicFieldName: Option[String],
                                        valueFieldsNames: Option[Seq[String]],
                                        stream: DataFrame,
                                        prototypeTopicModel: TopicModel) = {
    // there must be exactly one value field name and it must be a column of type string
    require(valueFieldsNames.isDefined && valueFieldsNames.get.size == 1,
            "Exactly one value field name must be defined for plaintext topic data type but zero or more than one " +
              s"were specified; value field names: ${valueFieldsNames.get.mkString("\"","\", \"","\"")}")
    val valueFieldName = valueFieldsNames.get.head
    val maybeValueColumn = stream.schema.find(_.name == valueFieldName)
    require(maybeValueColumn.isDefined,
            s"""The specified value field name "$valueFieldName" does not match any column; columns in schema: """ +
              s"""${stream.schema.map(_.name).mkString("[","], [","]")}""")
    val valueColumn = maybeValueColumn.get
    val valueColumnDataType = valueColumn.dataType
    require(valueColumnDataType == StringType,
            s"""The specified value field name "$valueFieldName" matches a column with a type that is not string; """ +
              s"incompatible type $valueColumnDataType found")
    
    // generate select expressions to rename matadata columns and convert everything to json
    val selectExpressions =
      keyFieldName.map(kfn => s"CAST($kfn AS binary) key").toList ++
        headersFieldName.map(hfn => s"$hfn AS headers").toList ++
        topicFieldName.map(tfn => s"$tfn AS topic").toList :+
        s"$valueFieldName AS value"
    
    logger.debug(s"Generated select expressions: ${selectExpressions.mkString("[", "], [", "]")}")
    
    // convert input
    stream.selectExpr(selectExpressions: _*)
  }
  
  private def convertStreamForBinary(keyFieldName: Option[String],
                                     headersFieldName: Option[String],
                                     topicFieldName: Option[String],
                                     valueFieldsNames: Option[Seq[String]],
                                     stream: DataFrame,
                                     prototypeTopicModel: TopicModel) = {
    // there must be exactly one value field name and it must be a column of type binary
    require(valueFieldsNames.isDefined && valueFieldsNames.get.size == 1,
            "Exactly one value field name must be defined for binary topic data type but zero or more than one were " +
              s"specified; value field names: ${valueFieldsNames.get.mkString("\"","\", \"","\"")}")
    val valueFieldName = valueFieldsNames.get.head
    val maybeValueColumn = stream.schema.find(_.name == valueFieldName)
    require(maybeValueColumn.isDefined,
            s"""The specified value field name "$valueFieldName" does not match any column; columns in schema: """ +
              s"""${stream.schema.map(_.name).mkString("[","], [","]")}""")
    val valueColumn = maybeValueColumn.get
    val valueColumnDataType = valueColumn.dataType
    require(valueColumnDataType == BinaryType,
            s"""The specified value field name "$valueFieldName" matches a column with a type that is not binary; """ +
              s"incompatible type $valueColumnDataType found")
    
    // generate select expressions to rename matadata columns and convert everything to json
    val selectExpressions =
      keyFieldName.map(kfn => s"CAST($kfn AS binary) key").toList ++
        headersFieldName.map(hfn => s"$hfn AS headers").toList ++
        topicFieldName.map(tfn => s"$tfn AS topic").toList :+
        s"$valueFieldName AS value"
    
    logger.debug(s"Generated select expressions: ${selectExpressions.mkString("[", "], [", "]")}")
    
    // convert input
    stream.selectExpr(selectExpressions: _*)
  }

  private def addTopicNameCheckIfNeeded(topicFieldName: Option[String], topics: Seq[TopicModel], stream: DataFrame) = {
    if (topicFieldName.isEmpty) {
      // no checks to be done as there is no per-row topic selection
      stream
    } else {
      // check that the topic specified appears in the models
      val acceptedTopicNames = topics.map(_.name).toSet
      val checkTopicName =
        (topicName: String) => {
          if (!acceptedTopicNames(topicName))
            throw new Exception(s"""Topic name "$topicName" is not in the topic models for the MultiTopicModel used""")
          else
            topicName
        }
      val checkTopicNameUdf = udf(checkTopicName)
      stream.withColumn("topic", checkTopicNameUdf(col("topic")))
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