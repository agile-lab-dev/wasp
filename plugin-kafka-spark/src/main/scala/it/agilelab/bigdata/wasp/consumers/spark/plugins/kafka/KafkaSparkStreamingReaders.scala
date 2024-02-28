package it.agilelab.bigdata.wasp.consumers.spark.plugins.kafka

import it.agilelab.bigdata.wasp.consumers.spark.plugins.kafka.TopicModelUtils.topicNameToColumnName
import it.agilelab.bigdata.wasp.consumers.spark.readers.SparkStructuredStreamingReader
import it.agilelab.bigdata.wasp.consumers.spark.utils.{AvroDeserializerExpression, SparkUtils}
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.WaspSystem.??
import it.agilelab.bigdata.wasp.core.kafka.CheckOrCreateTopic
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.utils._
import it.agilelab.bigdata.wasp.models._
import it.agilelab.bigdata.wasp.models.configuration.{Handle, Ignore, ParsingMode, Strict}
import it.agilelab.bigdata.wasp.repository.core.bl.{ConfigBL, TopicBL}
import it.agilelab.bigdata.wasp.spark.sql.kafka011.KafkaSparkSQLSchemas
import it.agilelab.darwin.manager.AvroSchemaManagerFactory
import org.apache.avro.Schema
import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.{CaseWhen, Hex}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.collection.mutable

object KafkaSparkStructuredStreamingReader extends SparkStructuredStreamingReader with Logging {
  private val KAFKA_METADATA_COL       = "kafkaMetadata"
  private val RAW_VALUE_ATTRIBUTE_NAME = "raw"
  private val DATA_TYPE_ATTRIBUTE_NAME = "dataType"

  /**
    * Creates a streaming DataFrame from a Kafka streaming source.
    *
    * If all the input topics share the same schema the returned DataFrame will contain a column named "kafkaMetadata"
    * with message metadata and the message contents either as a single column named "value" or as multiple columns
    * named after the value fields depending on the topic datatype.
    * If the input topics do not share the same schema the returned Dataframe will contain a column named
    * "kafkaMetadata" with message metadata and each topic content on a column named after the topic name,
    * previously escaped calling the function [[MultiTopicModel.topicModelNames()]].
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
    * <ul>
    * <li>key: bytes</li>
    * <li>headers: array of {headerKey: string, headerValue: bytes}</li>
    * <li>topic: string</li>
    * <li>partition: int</li>
    * <li>offset: long</li>
    * <li>timestamp: timestamp</li>
    * <li>timestampType: int</li>
    *</ul>
    * <br>
    * The behaviour for message contents column(s) is the following:
    * <ul>
    * <li>the "avro" and "json" topic data types will output the columns specified by their schemas</li>
    * <li>the "plaintext" and "bytes" topic data types output a "value" column with the contents as string or bytes respectively</li>
    * </ul>
    * <br>
    * There is also the possibility to manage a Parsing mode for Avro/json deserialization, this param can be:
    * <ul>
    * <li>Strict: job will crash when a record can't be parsed</li>
    * <li>Ignore: records that are impossible to parse will be filtered out</li>
    * <li>Handle: produce two columns instead of exploding the schema of parsed record, one containing raw bytes array,
    *     and the other containing the parsed value, if parsing it's ok, or null if parsing error has happened</li>
    * </ul>
    * <u>In Strict and Ignore mode result dataframe will have the same schema described early</u><br>
    * In Handle mode, for single topic when topic type is Avro/Json the structure will have:
    * <ul>
    *  <li>kafkaMetadata -> same as other mode</li>
    *  <li>raw           -> raw byte array, it will be null if parsing has worked fine, else it will contain raw byte array</li>
    *  <li>value         -> struct column that contains parsed record or null if parsing had problems</li>
    *  </ul>
    *  Here is example
    *  {{{
    * +--------------------+--------------------+-------------------------+
    * |       kafkaMetadata|                 raw|                    value|
    * +--------------------+--------------------+-------------------------+
    * |[45, [], test_jso...|          [45, 47..]|                     null|
    * |[12, [], testchec...|                null|      [12, 77, [field1_..|
    * +--------------------+--------------------+-------------------------+
    * }}}
    * <u>Handle mode is meant to divide the good data from bad data through a simple where(col("raw").isNull),
    * than good data can be exploded through select(col("value.*"))</u>
    * <p><b>N.B in Single reading mode Parsing Mode on binary/plaintext topic type will be ignored</b></p>
    *
    * <br>Handle parsing mode in multi mode scenario will be managed as standard multi mode plus the column "raw", which
    * has the same usage as single mode.
    * an example result is like:
    * {{{
    * +--------------------------------+----+------------+--------------+-----------------+-------------+
    *|                   kafkaMetadata| raw|  topic_json|  topic_binary|      topic_plain|    topic_avro|
    *+--------------------------------+----+------------+--------------+-----------------+--------------+
    *|[1, [], topic_json,.............|null| [1, valore]|          null|             null|          null|
    *|[1, [], topic_binary,...........|null|        null|   binary_test|             null|          null|
    *|[1, [], topic_plain,............|null|        null|          null|   plaintext_test|          null|
    *|[1, [], topic_avro,.............|[05]|        null|          null|             null|          null|
    *|[1, [], topic_avro,.............|null|        null|          null|             null|   [1, valore]|
    *+--------------------------------+----+------------+--------------+-----------------+--------------+
    * }}}
    * <u>In case of error raw column will be populated else is null,
    * to access parsed value is possible to select column "[topic_name]" this field will be null if parsing didn't work</u>
    * <b>
    *  <br><br>N.B. when a parsing error occurs, every topic_name column will have null as value, you can know one which one
    *  parsing error has occurred by looking at kafkaMetadata.topic field
    *  <br><br>N.B. for plaintext and binary type, since is possible to have topic_name.value = null, raw column will be null
    * as well (since is populated only for parsing errors which cannot happen on these types). </b>
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
        .withColumn(RAW_VALUE_ATTRIBUTE_NAME, col(KafkaSparkSQLSchemas.VALUE_ATTRIBUTE_NAME))

      parseDF(topics, df, streamingReaderModel.parsingMode)

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
            cName != KafkaSparkSQLSchemas.KEY_ATTRIBUTE_NAME &&
            cName != RAW_VALUE_ATTRIBUTE_NAME
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
    * topic names but will be sanitized by the function [[MultiTopicModel.topicModelNames]].
    */
  private def parseDF(topics: Seq[TopicModel], df: DataFrame, parsingMode: ParsingMode) = {
    MultiTopicModel.areTopicsHealthy(topics) match {
      case Left(a) => throw new IllegalArgumentException(a)
      case Right(_) =>
        TopicModelUtils.areTopicsEqualForReading(topics) match {
          case Left(a) =>
            TopicModelUtils.topicsShareKeySchema(topics) match {
              case Left(error) =>
                throw new IllegalArgumentException(error)
              case Right(_) =>
                logger.debug(s"Suppressing error: '$a' and trying with multipleSchema strategy")
                selectForMultipleSchema(topics, df, parsingMode)
            }
          case Right(_) =>
            selectForOneSchema(topics.head, df, parsingMode)
        }
    }
  }

  private[wasp] def selectForOneSchema(prototypeTopic: TopicModel, df: DataFrame, parsingMode: ParsingMode) = {
    val topicType = prototypeTopic.topicDataType
    if (parsingMode == Handle && !Seq(TopicDataTypes.JSON, TopicDataTypes.AVRO).contains(topicType)) {
      logger.warn(
        s"Handle parsing mode is not supported for $topicType topic type, it will be managed as in " +
          s"Strict mode (so no raw column will be produced). To remove this warning please set Strict as parsing mode " +
          s"of your topic. Look at KafkaSparkStructuredStreamingReader#createStructuredStream java doc for more details."
      )
    }
    val ret: DataFrame = topicType match {
      case TopicDataTypes.AVRO =>
        // parse avro bytes into a column, lift the contents up one level and push metadata into nested column
        val parsedDf = df.withColumn(KafkaSparkSQLSchemas.VALUE_ATTRIBUTE_NAME, parseAvroValue(prototypeTopic))
        checkParsingMode(parsedDf, parsingMode, TopicDataTypes.AVRO, parseKey(prototypeTopic))
      case TopicDataTypes.JSON =>
        val parsedDf = df.withColumn(
          KafkaSparkSQLSchemas.VALUE_ATTRIBUTE_NAME,
          from_json(parseString, getDataType(prototypeTopic.getJsonSchema))
        )
        checkParsingMode(parsedDf, parsingMode, TopicDataTypes.JSON)
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

  /**
    * Checks if parsing has happened correctly and behave differently basing on input parsing mode,
    * to check if a record has been parsed it checks that value column is not null :
    * <ul>
    * <li>Strict, launches an exception when unable to parse </li>
    * <li>Ignore, filters out record that are not been parsed</li>
    * <li>Handle, return raw column as null when parsing is good, else it's populated with original byte array</li>
    * </ul>
    *
    * @param df
    * @param parsingMode
    * @param topicDataType
    * @param metadataKey
    * @return metadata + exploded parsed fields for Strict and Ignore, metadata+ raw + value column when Handle mode,
    *         parsed values are in value column and can be exploded through value.*
    *  @throws SparkException when unable to parse a record in Strict mode
    */
  private[wasp] def checkParsingMode(
      df: DataFrame,
      parsingMode: ParsingMode,
      topicDataType: String,
      metadataKey: Column = col(KafkaSparkSQLSchemas.KEY_ATTRIBUTE_NAME)
  ): DataFrame = {
    parsingMode match {
      case Strict =>
        val computedValue = "computedValue"
        df.withColumn(
            computedValue,
            when(
              col(KafkaSparkSQLSchemas.VALUE_ATTRIBUTE_NAME).isNull,
              strictExceptionLauncherUdf(col(RAW_VALUE_ATTRIBUTE_NAME), lit(topicDataType))
            ).otherwise(col(KafkaSparkSQLSchemas.VALUE_ATTRIBUTE_NAME))
          )
          .select(selectMetadata(metadataKey), col(s"$computedValue.*"))
      case Ignore =>
        df.select(selectMetadata(metadataKey), col(s"${KafkaSparkSQLSchemas.VALUE_ATTRIBUTE_NAME}.*"))
          .where(col(KafkaSparkSQLSchemas.VALUE_ATTRIBUTE_NAME).isNotNull)
      case Handle =>
        df.select(
          selectMetadata(metadataKey),
          when(
            col(KafkaSparkSQLSchemas.VALUE_ATTRIBUTE_NAME).isNull,
            col(RAW_VALUE_ATTRIBUTE_NAME)
          ).otherwise(null)
            .as(RAW_VALUE_ATTRIBUTE_NAME),
          col(KafkaSparkSQLSchemas.VALUE_ATTRIBUTE_NAME)
        )
    }
  }

  /**
    * function that prints the content of the serialized value which is not deserializable,
    * it's useful to know which record caused the error
    *
    * @throws SparkException exception containing the record which has caused the parsing error
    */
  private[wasp] def strictExceptionLauncherUdf: UserDefinedFunction =
    udf((raw: Array[Byte], topicDataType: String) => {
      topicDataType match {
        case TopicDataTypes.JSON =>
          throw new SparkException(
            s"Unable to parse raw value [${StringToByteArrayUtil.byteArrayToString(raw)}] to $topicDataType"
          )
        case TopicDataTypes.AVRO =>
          throw new SparkException(s"Unable to parse raw value [${Hex.hex(raw)}] to $topicDataType")
      }
    })

  private[wasp] def selectForMultipleSchema(topics: Seq[TopicModel], df: DataFrame, parsingMode: ParsingMode) = {
    val valueColumns = topics
      .foldLeft(List.empty[Column]) { (cols, t) =>
        val dataType = t.topicDataType
        dataType match {
          case TopicDataTypes.AVRO =>
            parseIfMyTopicOrNull(t.name, parseAvroValue(t), dataType) :: cols
          case TopicDataTypes.JSON =>
            parseIfMyTopicOrNull(t.name, parseJson(t), dataType) :: cols
          case TopicDataTypes.PLAINTEXT =>
            parseIfMyTopicOrNull(t.name, parseString, dataType) :: cols
          case TopicDataTypes.BINARY =>
            // push metadata into nested column and keep value as is
            parseIfMyTopicOrNull(t.name, parseBinary, dataType) :: cols
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

    val ret = df.select(metadataExpr :: col(RAW_VALUE_ATTRIBUTE_NAME) :: valueColumns: _*)
    logger.debug(s"DataFrame schema before parsing mode check: ${ret.schema.treeString}")
    val retChecked = checkParsingModeMultipleTopics(ret, parsingMode)
    logger.debug(s"DataFrame schema after parsing mode check: ${ret.schema.treeString}")
    retChecked
  }

  /**
    * Checks if parsing has happened correctly and behave differently basing on input parsing mode,
    * to check if a record has been parsed it checks that topicName.value column is not null :
    * <ul>
    *   <li>Strict, launches an exception when unable to parse </li>
    *   <li>Ignore, filters out record that are not been parsed</li>
    *   <li>Handle, return raw column as null when parsing is good, else populated with original byte array</li>
    * </ul>
    * @param df
    * @param parsingMode
    * @return df parsed according to input parsing mode
    * @throws SparkException
    */
  private[wasp] def checkParsingModeMultipleTopics(df: DataFrame, parsingMode: ParsingMode) = {
    val parsedCols = df.columns.diff(Seq(KAFKA_METADATA_COL, RAW_VALUE_ATTRIBUTE_NAME))

    def parsedValueCol(colName: String) = col(s"$colName.${KafkaSparkSQLSchemas.VALUE_ATTRIBUTE_NAME}")

    def parsedDataTypeCol(colName: String) = col(s"$colName.$DATA_TYPE_ATTRIBUTE_NAME")

    //check for extra safety, binary and plaintext type should not be null
    def dataTypeToCheckCondition(colName: String) =
      col(colName).isNull || lower(parsedDataTypeCol(colName)).isin(TopicDataTypes.JSON, TopicDataTypes.AVRO)

    val goodCaseSelect = parsedCols.map(c => when(col(c).isNotNull, parsedValueCol(c)).otherwise(null).as(c))
    val parsingErrorFilteredCondition = parsedCols
      .map(c =>
        col(c).isNull || !dataTypeToCheckCondition(c) || (dataTypeToCheckCondition(c) && parsedValueCol(c).isNotNull)
      )
      .reduce(_ and _)

    parsingMode match {
      case Strict =>
        parsedCols
          .foldLeft(df) { (df, c) =>
            df.withColumn(
              "workingColumn",
              when(
                dataTypeToCheckCondition(c) && col(c).isNotNull && parsedValueCol(c).isNull,
                strictExceptionLauncherUdf(col(RAW_VALUE_ATTRIBUTE_NAME), parsedDataTypeCol(c))
              ).otherwise(null)
            )
          }
          .select(col(KAFKA_METADATA_COL) +: goodCaseSelect: _*)
      case Ignore =>
        df.where(parsingErrorFilteredCondition).select(col(KAFKA_METADATA_COL) +: goodCaseSelect: _*)

      case Handle =>
        val rawCol =
          when(!parsingErrorFilteredCondition, col(RAW_VALUE_ATTRIBUTE_NAME))
            .otherwise(null)
            .as(RAW_VALUE_ATTRIBUTE_NAME)
        df.select(Seq(col(KAFKA_METADATA_COL), rawCol) ++ goodCaseSelect: _*)
    }

  }

  private def parseBinary = {
    col(RAW_VALUE_ATTRIBUTE_NAME)
  }

  private def parseString = {
    col(RAW_VALUE_ATTRIBUTE_NAME).cast(StringType)
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
            sm   <- avroSchemaManager
            subj <- SubjectStrategy.subjectFor(t.getJsonSchema, t, true)
          } yield {
            val idAndSchema = sm
              .retrieveLatestSchema(subj)
              .getOrElse(
                throw new RuntimeException(
                  s"Reader schema not specified and fetching latest schema with subject '${subj}' failed."
                )
              )
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
        sm   <- avroSchemaManager
        subj <- SubjectStrategy.subjectFor(t.getJsonSchema, t, false)
      } yield {
        val idAndSchema = sm
          .retrieveLatestSchema(subj)
          .getOrElse(
            throw new RuntimeException(
              s"Reader schema not specified and fetching latest schema with subject '${subj}' failed."
            )
          )
        idAndSchema._2.toString
      }
    } else {
      Some(t.getJsonSchema)
    }

    val avroToRowConversion = AvroDeserializerExpression(
      col(RAW_VALUE_ATTRIBUTE_NAME).expr,
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

  /**
    * @param topicName topic name which value should match topic column value
    * @param parseCol  column which contains parsed value
    * @param dataType  input schema for data in the topic
    * @return null if the current row doesn't match requested topic, else a struct of parsed value as 'value'
    *         and topic encoding data type
    */
  private def parseIfMyTopicOrNull(topicName: String, parseCol: Column, dataType: String): Column = {
    def when(condition: Column, value: Column): Column = new Column(CaseWhen(Seq((condition.expr, value.expr))))

    when(
      col(KafkaSparkSQLSchemas.TOPIC_ATTRIBUTE_NAME).equalTo(topicName),
      struct(parseCol.as(KafkaSparkSQLSchemas.VALUE_ATTRIBUTE_NAME), lit(dataType).as(DATA_TYPE_ATTRIBUTE_NAME))
    ).otherwise(null)
      .as(topicNameToColumnName(topicName))

  }

}
