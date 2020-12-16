package it.agilelab.bigdata.wasp.consumers.spark.plugins.kafka

import it.agilelab.bigdata.wasp.consumers.spark.readers.{SparkLegacyStreamingReader, SparkStructuredStreamingReader}
import it.agilelab.bigdata.wasp.consumers.spark.utils.{AvroDeserializerExpression, SparkUtils}
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.WaspSystem.??
import it.agilelab.bigdata.wasp.repository.core.bl.{ConfigBL, TopicBL}
import it.agilelab.bigdata.wasp.core.kafka.CheckOrCreateTopic
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.utils._
import it.agilelab.bigdata.wasp.models.{MultiTopicModel, StreamingReaderModel, StructuredStreamingETLModel, TopicModel}
import it.agilelab.bigdata.wasp.spark.sql.kafka011.KafkaSparkSQLSchemas._
import org.apache.avro.Schema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

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
  override def createStructuredStream(etl: StructuredStreamingETLModel, streamingReaderModel: StreamingReaderModel)(
      implicit ss: SparkSession
  ): DataFrame = {

    logger.info(s"Creating stream from input: $streamingReaderModel of ETL: $etl")

    // extract the topic model
    logger.info(s"""Retrieving topic datastore model with name "${streamingReaderModel.datastoreModelName}"""")
    val topicBL = ConfigBL.topicBL
    val topics  = retrieveTopicModelsRecursively(topicBL, streamingReaderModel.datastoreModelName)
    MultiTopicModel.validateTopicModels(topics)
    logger.info(s"Retrieved topic model(s): $topics")

    // get the config
    val kafkaConfig = ConfigManager.getKafkaConfig
    logger.info(s"Kafka configuration: $kafkaConfig")

    // check or create
    val allCheckOrCreateResult = topics map { topic =>
      ??[Boolean](WaspSystem.kafkaAdminActor, CheckOrCreateTopic(topic.name, topic.partitions, topic.replicas))
    } reduce (_ && _)

    if (allCheckOrCreateResult) {
      // extract a prototype topic to use for data type, schema etc
      val prototypeTopic = topics.head

      // calculate maxOffsetsPerTrigger from trigger interval and rate limit
      // if the rate limit is set, calculate maxOffsetsPerTrigger as follows: if the trigger interval is unset, use
      // rate limit as is, otherwise multiply by triggerIntervalMs/1000
      // if the rate limit is not set, do not set maxOffsetsPerTrigger
      val triggerIntervalMs = SparkUtils.getTriggerIntervalMs(ConfigManager.getSparkStreamingConfig, etl)
      val maybeRateLimit: Option[Long] = streamingReaderModel.rateLimit.map(x =>
        if (triggerIntervalMs == 0L) x else (triggerIntervalMs / 1000d * x).toLong
      )
      val maybeMaxOffsetsPerTrigger = maybeRateLimit.map(rateLimit => ("maxOffsetsPerTrigger", rateLimit.toString))

      // calculate the options for the DataStreamReader
      val options = mutable.Map.empty[String, String]
      // start with the base options
      options ++= Seq(
        "subscribe"                   -> topics.map(_.name).mkString(","),
        "kafka.bootstrap.servers"     -> kafkaConfig.connections.map(_.toString).mkString(","),
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
          val darwinConf = if (prototypeTopic.useAvroSchemaManager) {
            Some(ConfigManager.getAvroSchemaManagerConfig)
          } else {
            None
          }

          val avroToRowConversion = AvroDeserializerExpression(
            col("value").expr,
            prototypeTopic.getJsonSchema,
            darwinConf,
            avoidReevaluation = true
          )

          // parse avro bytes into a column, lift the contents up one level and push metadata into nested column
          df.withColumn("value_parsed", new Column(avroToRowConversion))
            .drop("value")
            .selectExpr(metadataSelectExpr, "value_parsed.*")
        }
        case "json" => {
          // prepare the udf
          val byteArrayToJson: Array[Byte] => String = StringToByteArrayUtil.byteArrayToString
          val byteArrayToJsonUDF                     = udf(byteArrayToJson)

          // convert bytes to json string, parse the json into a column, lift the contents up one level and push
          // metadata into nested column
          df.withColumn("value_parsed", byteArrayToJsonUDF(col("value")))
            .drop("value")
            .withColumn("value", from_json(col("value_parsed"), getDataType(prototypeTopic.getJsonSchema)))
            .selectExpr(metadataSelectExpr, "value.*")
        }
        case "plaintext" => {
          // prepare the udf
          val byteArrayToString: Array[Byte] => String = StringToByteArrayUtil.byteArrayToString
          val byteArrayToStringUDF                     = udf(byteArrayToString)

          // convert bytes to string and push metadata into nested column
          df.withColumn("value_string", byteArrayToStringUDF(col("value")))
            .selectExpr(metadataSelectExpr, "value_string AS value")
        }
        case "binary" => {
          // push metadata into nested column and keep value as is
          df.selectExpr(metadataSelectExpr, "value")
        }
        case _ =>
          throw new UnsupportedOperationException(s"""Unsupported topic data type "${prototypeTopic.topicDataType}"""")
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
          multiTopicModel.topicModelNames
            .flatMap(innerRetrieveTopicModelsRecursively)
      }
    }

    innerRetrieveTopicModelsRecursively(topicDatastoreModelName)
  }

  private def getDataType(schema: String): DataType = {
    val schemaAvro = new Schema.Parser().parse(schema)
    AvroSchemaConverters.toSqlType(schemaAvro).dataType
  }
}

object KafkaSparkLegacyStreamingReader extends SparkLegacyStreamingReader with Logging {

  /**
    * Kafka configuration
    */
  //TODO: check warning (not understood)
  def createStream(group: String, accessType: String, topic: TopicModel)(
      implicit ssc: StreamingContext
  ): DStream[String] =
    throw new Exception("Legacy Streaming was removed, migrate to structured streaming")

}
