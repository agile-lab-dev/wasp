package it.agilelab.bigdata.wasp.consumers.spark.plugins.kafka

import it.agilelab.bigdata.wasp.consumers.spark.readers.{SparkLegacyStreamingReader, SparkStructuredStreamingReader}
import it.agilelab.bigdata.wasp.consumers.spark.utils.{AvroDeserializerExpression, SparkUtils}
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.WaspSystem.??
import it.agilelab.bigdata.wasp.core.kafka.CheckOrCreateTopic
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.utils._
import it.agilelab.bigdata.wasp.models.MultiTopicModel.topicNameToColumnName
import it.agilelab.bigdata.wasp.models._
import it.agilelab.bigdata.wasp.repository.core.bl.{ConfigBL, TopicBL}
import it.agilelab.bigdata.wasp.spark.sql.kafka011.KafkaSparkSQLSchemas
import it.agilelab.darwin.manager.AvroSchemaManagerFactory
import org.apache.avro.Schema
import org.apache.spark.sql.catalyst.expressions.CaseWhen
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable

object KafkaSparkStructuredStreamingReader extends SparkStructuredStreamingReader with Logging {
  private val KAFKA_METADATA_COL = "kafkaMetadata"

  /**
    * Creates a streaming DataFrame from a Kafka streaming source.
    *
    * If all the input topics share the same schema the returned DataFrame will contain a column named "kafkaMetadata"
    * with message metadata and the message contents either as a single column named "value" or as multiple columns
    * named after the value fields depending on the topic datatype.
    * If the input topics do not share the same schema the returned Dataframe will contain a column named
    * "kafkaMetadata" with message metadata and each topic content on a column named after the topic name,
    * previously escaped calling the function [[MultiTopicModel.topicNameToColumnName()]].
    * This means that if 5 topic models with different schema are read, the output dataframe will contain 6 columns,
    * and of these 6 columns only the kafkaMetadata and the topic related to that message one, will have a value
    * different from null, like the following:
    *
    * {{{
    * +--------------------+--------------------+-------------------------+
    * |       kafkaMetadata|     test_json_topic|testcheckpoint_avro_topic|
    * +--------------------+--------------------+-------------------------+
    * |[45, [], test_jso...|[45, 45, [field1_...|                     null|
    * |[12, [], testchec...|                null|      [12, 77, [field1_..|
    * +--------------------+--------------------+-------------------------+
    * }}}
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

    logger.info(s"Retrieved topic model(s): $topics")

    // get the config
    val kafkaConfig = ConfigManager.getKafkaConfig
    logger.info(s"Kafka configuration: $kafkaConfig")

    // check or create
    val allCheckOrCreateResult = topics map { topic =>
      ??[Boolean](WaspSystem.kafkaAdminActor, CheckOrCreateTopic(topic.name, topic.partitions, topic.replicas))
    } reduce (_ && _)

    if (allCheckOrCreateResult) {
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

      parseDF(topics, df)

    } else {
      val msg = s"Unable to check/create one or more topic; topics: $topics"
      logger.error(msg)
      throw new Exception(msg)
    }
  }

  private def selectMetadata(keyCol: Column = col(KafkaSparkSQLSchemas.KEY_ATTRIBUTE_NAME)): Column = {
    // find all kafka metadata (non-value) columns so we can keep them in the final select
    val allColumnsButValue =
      KafkaSparkSQLSchemas.INPUT_SCHEMA
        .map(_.name)
        .filter(cName =>
          cName != KafkaSparkSQLSchemas.VALUE_ATTRIBUTE_NAME &&
            cName != KafkaSparkSQLSchemas.KEY_ATTRIBUTE_NAME
        )
        .toList

    // create select expression to push all kafka metadata columns under a single complex column called
    // "kafkaMetadata"
    val metadataSelectExpr = struct(keyCol :: allColumnsButValue.map(col): _*).as(KAFKA_METADATA_COL)
    metadataSelectExpr
  }

  /**
    * Throws IllegalArgumentException if the topics are malformed, otherwise checks if all the topics have the same
    * schema, if they do, it parses the messages according to the only schema and returns a dataframe
    * with a kafkaMetadata column and all the columns that the schema defines. If the topic is well defined but
    * multiple schemas are encountered it will return a dataframe with a kafkaMetadata columns plus a column for each
    * different topic name which will contain the parsed data from that topic. This means that if there are 10 different
    * topics to fetch with 10 different schemas the dataframe will have 11 columns and every row will contain 9 null
    * values, one column with the parsed message and one with the kafka metadata. The column names will reflect the
    * topic names but will be sanitized by the function [[MultiTopicModel.topicNameToColumnName()]].
    */
  private def parseDF(topics: Seq[TopicModel], df: DataFrame) = {
    MultiTopicModel.areTopicsHealthy(topics) match {
      case Left(a) => throw new IllegalArgumentException(a)
      case Right(_) =>
        MultiTopicModel.areTopicsEqualForReading(topics) match {
          case Left(a) =>
            MultiTopicModel.topicsShareKeySchema(topics) match {
              case Left(error) =>
                throw new IllegalArgumentException(error)
              case Right(_) =>
                logger.debug(s"Suppressing error: '$a' and trying with multipleSchema strategy")
                selectForMultipleSchema(topics, df)
            }
          case Right(_) =>
            selectForOneSchema(topics.head, df)
        }
    }
  }

  private[wasp] def selectForOneSchema(prototypeTopic: TopicModel, df: DataFrame) = {
    val ret: DataFrame = prototypeTopic.topicDataType match {
      case TopicDataTypes.AVRO =>
        // parse avro bytes into a column, lift the contents up one level and push metadata into nested column
        df.withColumn("value_parsed", parseAvroValue(prototypeTopic))
          .drop(KafkaSparkSQLSchemas.VALUE_ATTRIBUTE_NAME)
          .select(selectMetadata(parseKey(prototypeTopic)), expr("value_parsed.*"))
      case TopicDataTypes.JSON =>
        df.withColumn(
            KafkaSparkSQLSchemas.VALUE_ATTRIBUTE_NAME,
            from_json(parseString, getDataType(prototypeTopic.getJsonSchema))
          )
          .select(selectMetadata(), expr("value.*"))
      case TopicDataTypes.PLAINTEXT =>
        df.withColumn("value_string", parseString)
          .select(selectMetadata(), expr("value_string AS value"))
      case TopicDataTypes.BINARY =>
        // push metadata into nested column and keep value as is
        df.select(selectMetadata(), expr(KafkaSparkSQLSchemas.VALUE_ATTRIBUTE_NAME))
      case _ =>
        throw new UnsupportedOperationException(s"""Unsupported topic data type "${prototypeTopic.topicDataType}"""")
    }
    logger.debug(s"DataFrame schema: ${ret.schema.treeString}")
    ret
  }

  private[wasp] def selectForMultipleSchema(topics: Seq[TopicModel], df: DataFrame) = {
    val valueColumns = topics
      .foldLeft(List.empty[Column]) { (cols, t) =>
        t.topicDataType match {
          case TopicDataTypes.AVRO =>
            parseIfMyTopicOrNull(t.name, parseAvroValue(t)) :: cols
          case TopicDataTypes.JSON =>
            parseIfMyTopicOrNull(t.name, parseJson(t)) :: cols
          case TopicDataTypes.PLAINTEXT =>
            parseIfMyTopicOrNull(t.name, parseString) :: cols
          case TopicDataTypes.BINARY =>
            // push metadata into nested column and keep value as is
            parseIfMyTopicOrNull(t.name, parseBinary) :: cols
          case _ =>
            throw new UnsupportedOperationException(s"""Unsupported topic data type "${t.topicDataType}"""")
        }
      }
      .reverse
    logger.info(
      s"Selecting the following columns:\n${valueColumns.mkString("\t- ", "\n\t- ", "")}"
    )

    val metadataExpr = topics.collectFirst {
      case t @ TopicModel(_, _, _, _, TopicDataTypes.AVRO, _, _, _, _, _, _, _, _) => t
    } match {
      case Some(t) => selectMetadata(parseKey(t))
      case None    => selectMetadata()
    }

    val ret = df.select(metadataExpr :: valueColumns: _*)
    logger.debug(s"DataFrame schema: ${ret.schema.treeString}")
    ret
  }

  private def parseBinary = {
    col(KafkaSparkSQLSchemas.VALUE_ATTRIBUTE_NAME)
  }

  private def parseString = {
    col(KafkaSparkSQLSchemas.VALUE_ATTRIBUTE_NAME).cast(StringType)
  }

  private def parseJson(t: TopicModel) = {
    from_json(parseString, getDataType(t.getJsonSchema))
  }
  // expression that parses the key of the topic.
  // if the key does not have a schema is left binary as-is.
  // if the key has a schema is parsed as avro using it
  private def parseKey(t: TopicModel) = {
    t.keySchema match {
      case Some(keySchema) =>
        logger.debug(s"AVRO key schema: ${new Schema.Parser().parse(keySchema).toString(true)}")
        val darwinConf = if (t.useAvroSchemaManager) {
          Some(ConfigManager.getAvroSchemaManagerConfig)
        } else {
          None
        }

        lazy val avroSchemaManager = darwinConf.map(AvroSchemaManagerFactory.initialize)

        val schemaToUse = if (keySchema.isEmpty) {
          for {
            sm <- avroSchemaManager
            subj <- SubjectStrategy.subjectFor(t.getJsonSchema, t, true)
          } yield {
            val idAndSchema = sm.retrieveLatestSchema(subj).getOrElse(throw new RuntimeException(s"Reader schema not specified and fetching latest schema with subject '${subj}' failed."))
            idAndSchema._2.toString
          }
        } else {
          Some(keySchema)
        }

        val avroKeyConversion = AvroDeserializerExpression(
          col(KafkaSparkSQLSchemas.KEY_ATTRIBUTE_NAME).expr,
          schemaToUse.get,
          darwinConf,
          avoidReevaluation = true
        )
        new Column(avroKeyConversion)
      case None =>
        col(KafkaSparkSQLSchemas.KEY_ATTRIBUTE_NAME)
    }
  }
  private def parseAvroValue(t: TopicModel): Column = {
    logger.debug(s"AVRO value schema: ${new Schema.Parser().parse(t.getJsonSchema).toString(true)}")
    val darwinConf = if (t.useAvroSchemaManager) {
      Some(ConfigManager.getAvroSchemaManagerConfig)
    } else {
      None
    }

    lazy val avroSchemaManager = darwinConf.map(AvroSchemaManagerFactory.initialize)

    val schemaToUse = if (t.schema.isEmpty) {
      for {
        sm <- avroSchemaManager
        subj <- SubjectStrategy.subjectFor(t.getJsonSchema, t, false)
      } yield {
        val idAndSchema = sm.retrieveLatestSchema(subj).getOrElse(throw new RuntimeException(s"Reader schema not specified and fetching latest schema with subject '${subj}' failed."))
        idAndSchema._2.toString
      }
    } else {
      Some(t.getJsonSchema)
    }

    val avroToRowConversion = AvroDeserializerExpression(
      col(KafkaSparkSQLSchemas.VALUE_ATTRIBUTE_NAME).expr,
      schemaToUse.get,
      darwinConf,
      avoidReevaluation = true
    )
    new Column(avroToRowConversion)
  }

  private def retrieveTopicModelsRecursively(topicBL: TopicBL, topicDatastoreModelName: String): Seq[TopicModel] = {
    def innerRetrieveTopicModelsRecursively(topicDatastoreModelName: String): Seq[TopicModel] = {
      val topicDatastoreModel = topicBL
        .getByName(topicDatastoreModelName)
        .getOrElse(throw new IllegalArgumentException(s"Cannot find topic with name: $topicDatastoreModelName"))
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

  private def parseIfMyTopicOrNull(topicName: String, parseCol: Column): Column = {
    def when(condition: Column, value: Column): Column = new Column(CaseWhen(Seq((condition.expr, value.expr))))
    when(col(KafkaSparkSQLSchemas.TOPIC_ATTRIBUTE_NAME).equalTo(topicName), parseCol)
      .otherwise(null)
      .as(topicNameToColumnName(topicName))
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
