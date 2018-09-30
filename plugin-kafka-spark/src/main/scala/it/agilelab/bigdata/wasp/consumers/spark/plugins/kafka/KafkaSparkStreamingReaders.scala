package it.agilelab.bigdata.wasp.consumers.spark.plugins.kafka

import it.agilelab.bigdata.wasp.consumers.spark.readers.{SparkLegacyStreamingReader, SparkStructuredStreamingReader}
import it.agilelab.bigdata.wasp.consumers.spark.utils.SparkUtils
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.WaspSystem.??
import it.agilelab.bigdata.wasp.core.bl.{TopicBL, TopicBLImp}
import it.agilelab.bigdata.wasp.core.kafka.CheckOrCreateTopic
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.{MultiTopicModel, StreamingReaderModel, StructuredStreamingETLModel, TopicModel}
import it.agilelab.bigdata.wasp.core.utils._
import it.agilelab.bigdata.wasp.spark.sql.kafka011.KafkaSparkSQLSchemas._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.avro.Schema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.collection.mutable

object KafkaSparkStructuredStreamingReader extends SparkStructuredStreamingReader with Logging {

  /**
    * Creates a streaming DataFrame from a Kafka streaming source.
    *
    * The returned DataFrame will contain a column named "kafkaMetadata" column with message metadata and the message
    * contents either as a single column named "value" or as multiple columns named after the value contents depending
    * on the topic data type.
    *
    * The "kafkaMetadata" column contains the following:
    * - key: bytes
    * - headers: array of {headerKey: string, headerValue: bytes}
    * - topic: string
    * - partition: int
    * - offset: long
    * - timestamp: timestamp
    * - timestampType: int
    *
    * The behaviour for message contents column(s) is the following:
    * - the "avro" and "json" topic data types will output the columns specified by their schemas
    * - the "plaintext" and "bytes" topic data types output a "value" column with the contents as string or bytes respectively
    */
  override def createStructuredStream(etl: StructuredStreamingETLModel,
                                      streamingReaderModel: StreamingReaderModel)
                                     (implicit ss: SparkSession): DataFrame = {

    logger.info(s"Creating stream from input: $streamingReaderModel of ETL: $etl")
    
    // extract the topic model
    logger.info(s"""Retrieving topic datastore model with name "${streamingReaderModel.datastoreModelName}"""")
    val topicBL = new TopicBLImp(WaspDB.getDB)
    val topics = retrieveTopicModelsRecursively(topicBL, streamingReaderModel.datastoreModelName)
    MultiTopicModel.validateTopicModels(topics)
    logger.info(s"Retrieved topic model(s): $topics")
    
    // get the config
    val kafkaConfig = ConfigManager.getKafkaConfig
    logger.info(s"Kafka configuration: $kafkaConfig")

    // check or create
    val allCheckOrCreateResult = topics map { topic =>
      ??[Boolean](
        WaspSystem.kafkaAdminActor,
        CheckOrCreateTopic(topic.name, topic.partitions, topic.replicas))
    } reduce (_ && _)
    
    if (allCheckOrCreateResult) {
      // extract a prototype topic to use for data type, schema etc
      val prototypeTopic = topics.head
      
      // calculate maxOffsetsPerTrigger from trigger interval and rate limit
      // if the rate limit is set, calculate maxOffsetsPerTrigger as follows: if the trigger interval is unset, use
      // rate limit as is, otherwise multiply by triggerIntervalMs/1000
      // if the rate limit is not set, do not set maxOffsetsPerTrigger
      val triggerIntervalMs = SparkUtils.getTriggerIntervalMs(ConfigManager.getSparkStreamingConfig, etl)
      val maybeRateLimit = streamingReaderModel.rateLimit.map(x => if (triggerIntervalMs == 0l) x else triggerIntervalMs/1000d * x)
      val maybeMaxOffsetsPerTrigger = maybeRateLimit.map(rateLimit => ("maxOffsetsPerTrigger", rateLimit.toString))
      
      // calculate the options for the DataStreamReader
      val options = mutable.Map.empty[String, String]
      // start with the base options
      options ++= Seq(
        "subscribe" -> topics.map(_.name).mkString(","),
        "kafka.bootstrap.servers" -> kafkaConfig.connections.map(_.toString).mkString(","),
        "kafkaConsumer.pollTimeoutMs" -> kafkaConfig.ingestRateToMills().toString
      )
      // apply rate limit if it exists
      options ++= maybeMaxOffsetsPerTrigger
      // layer on the options coming from the kafka config "others" field
      options ++= kafkaConfig.others.map(_.toTupla).toMap
      // layer on the options coming from the streamingReaderModel
      options ++= streamingReaderModel.options
      logger.info(s"Final options to be pushed to DataStreamReader: $options")

      // create the stream
      val df: DataFrame = ss.readStream
        .format("kafka")
        .options(options)
        .load()
  
      // find all kafka metadata (non-value) columns so we can keep them in the final select
      val allColumnsButValue = INPUT_SCHEMA.map(_.name).filter(_ != VALUE_ATTRIBUTE_NAME)
  
      // create select expression to push all kafka metadata columns under a single complex column called
      // "kafkaMetadata"
      val metadataSelectExpr = s"struct(${allColumnsButValue.mkString(", ")}) as kafkaMetadata"
      
      val ret: DataFrame = prototypeTopic.topicDataType match {
        case "avro" => {
          logger.debug(s"AVRO schema: ${new Schema.Parser().parse(prototypeTopic.getJsonSchema).toString(true)}")
          
          // prepare the udf
          val avroToRow = AvroToRow(prototypeTopic.getJsonSchema)
          val avroToRowConversion: Array[Byte] => Row = avroToRow.read
          val avroToRowConversionUDF = udf(avroToRowConversion, avroToRow.getSchemaSpark())

          // parse avro bytes into a column, lift the contents up one level and push metadata into nested column
          df.withColumn("value_parsed", avroToRowConversionUDF(col("value")))
            .drop("value")
            .selectExpr(metadataSelectExpr, "value_parsed.*")
        }
        case "json" => {
          // prepare the udf
          val byteArrayToJson: Array[Byte] => String = StringToByteArrayUtil.byteArrayToString
          val byteArrayToJsonUDF = udf(byteArrayToJson)
          
          // convert bytes to json string, parse the json into a column, lift the contents up one level and push
          // metadata into nested column
          df.withColumn("value_parsed", byteArrayToJsonUDF(col("value")))
            .drop("value")
            .withColumn("value", from_json(col("value_parsed"), prototypeTopic.getDataType))
            .selectExpr(metadataSelectExpr, "value.*")
        }
        case "plaintext" => {
          // prepare the udf
          val byteArrayToString: Array[Byte] => String = StringToByteArrayUtil.byteArrayToString
          val byteArrayToStringUDF = udf(byteArrayToString)
          
          // convert bytes to string and push metadata into nested column
          df.withColumn("value_parsed", byteArrayToStringUDF(col("value")))
            .selectExpr(metadataSelectExpr, "value")
        }
        case _ => throw new UnsupportedOperationException(s"""Unsupported topic data type "${prototypeTopic.topicDataType}"""")
      }
  
      logger.debug(s"DataFrame schema: ${ret.schema.treeString}")
      
      ret
    } else {
      val msg = s"Unable to check/create one or more topic; topics: $topics"
      logger.error(msg)
      throw new Exception(msg)
    }
  }
  
  private def retrieveTopicModelsRecursively(topicBL: TopicBL, topicDatastoreModelName: String): Seq[TopicModel] = {
    def innerRetrieveTopicModelsRecursively(topicDatastoreModelName: String): Seq[TopicModel] = {
      val topicDatastoreModel = topicBL.getByName(topicDatastoreModelName).get
      topicDatastoreModel match {
        case topicModel: TopicModel => Seq(topicModel)
        case multiTopicModel: MultiTopicModel =>
          multiTopicModel
            .topicModelNames
            .flatMap(innerRetrieveTopicModelsRecursively)
      }
    }
    
    innerRetrieveTopicModelsRecursively(topicDatastoreModelName)
  }
}

object KafkaSparkLegacyStreamingReader extends SparkLegacyStreamingReader with Logging {

  /**
    * Kafka configuration
    */
  //TODO: check warning (not understood)
  def createStream(group: String, accessType: String, topic: TopicModel)(
      implicit ssc: StreamingContext): DStream[String] = {
    val kafkaConfig = ConfigManager.getKafkaConfig

    val kafkaConfigMap: Map[String, String] = (
      Seq(
        "zookeeper.connect" -> kafkaConfig.zookeeperConnections.toString(),
        "zookeeper.connection.timeout.ms" ->
          kafkaConfig.zookeeperConnections.connections.headOption.flatMap(_.timeout)
            .getOrElse(ConfigManager.getWaspConfig.servicesTimeoutMillis)
            .toString) ++
        kafkaConfig.others.map(_.toTupla)
      )
      .toMap

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
      val topicSchema = JsonConverter.toString(topic.schema.asDocument())
      topic.topicDataType match {
        case "avro" =>
          receiver.map(x => (x._1, AvroToJsonUtil.avroToJson(x._2, topicSchema))).map(_._2)
        case "json" | "plaintext" =>
          receiver
            .map(x => (x._1, StringToByteArrayUtil.byteArrayToString(x._2)))
            .map(_._2)
        case _ =>
          receiver.map(x => (x._1, AvroToJsonUtil.avroToJson(x._2, topicSchema))).map(_._2)
      }

    } else {
      val msg = s"Topic not found on Kafka: $topic"
      logger.error(msg)
      throw new Exception(msg)
    }
  }
}