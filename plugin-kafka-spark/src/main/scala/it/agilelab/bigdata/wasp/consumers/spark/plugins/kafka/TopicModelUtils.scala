package it.agilelab.bigdata.wasp.consumers.spark.plugins.kafka

import com.typesafe.config.Config
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.WaspSystem.??
import it.agilelab.bigdata.wasp.core.kafka.CheckOrCreateTopic
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import it.agilelab.bigdata.wasp.models.MultiTopicModel.areTopicsHealthy
import it.agilelab.bigdata.wasp.models.configuration.TinyKafkaConfig
import it.agilelab.bigdata.wasp.models._
import it.agilelab.bigdata.wasp.repository.core.bl.TopicBL
import it.agilelab.bigdata.wasp.utils.EitherUtils.{catchNonFatal, traverse}
import org.apache.avro.Schema
import org.apache.spark.sql.{Column, DataFrame}
import it.agilelab.bigdata.wasp.utils.EitherUtils._
import scala.collection.JavaConverters._

object TopicModelUtils extends Logging {

  private[kafka] def askToCheckOrCreateTopics(topics: Seq[TopicModel]): Unit = {
    logger.info(s"Creating topics $topics")

    topics.foreach(topic =>
      if (! ??[Boolean](WaspSystem.kafkaAdminActor, CheckOrCreateTopic(topic.name, topic.partitions, topic.replicas)))
        throw new Exception(s"""Error creating topic "${topic.name}"""")
    )
  }

  sealed trait FieldNameAndTopicModels
  case class SingleTopic(t: TopicModel)                                  extends FieldNameAndTopicModels
  case class MultiTopic(topicFieldName: String, topics: Seq[TopicModel]) extends FieldNameAndTopicModels

  private[kafka] def retrieveTopicFieldNameAndTopicModels(
      dsModel: DatastoreModel,
      topicBL: TopicBL,
      topicDatastoreModelName: String
  ): FieldNameAndTopicModels = {
    dsModel match {
      case t: TopicModel => SingleTopic(t)
      case multiTopicModel: MultiTopicModel =>
        val topics = multiTopicModel.topicModelNames
          .map(topicBL.getByName)
          .flatMap {
            case Some(topicModel: TopicModel) =>
              Seq(topicModel)
            case _ =>
              throw new Exception(s"""Unable to retrieve topic datastore model with name "$topicDatastoreModelName"""")
          }
        MultiTopic(multiTopicModel.topicNameField, topics)
      case other => throw new Exception(s"Datastore model $other is not compatible with Kafka")
    }
  }

  def retrieveKafkaTopicSettings(topicBL: TopicBL, topicDatastoreModelName: String): KafkaTopicSettings = {
    val tinyKafkaConfig = ConfigManager.getKafkaConfig.toTinyConfig()
    val mainTopicModel = topicBL
      .getByName(topicDatastoreModelName)
      .getOrElse(
        throw new Exception(s"""Unable to retrieve topic datastore model with name "$topicDatastoreModelName"""")
      )

    val topicFieldNameAndTopics =
      retrieveTopicFieldNameAndTopicModels(mainTopicModel, topicBL, topicDatastoreModelName)

    topicFieldNameAndTopics match {
      case SingleTopic(t) =>
        askToCheckOrCreateTopics(Seq(t))
        val darwinConf = if (t.useAvroSchemaManager) {
          Some(ConfigManager.getAvroSchemaManagerConfig)
        } else {
          None
        }
        KafkaTopicSettings(tinyKafkaConfig, mainTopicModel, None, Seq.empty, darwinConf)

      case MultiTopic(topicFieldName, topics) =>
        askToCheckOrCreateTopics(topics)
        val darwinConf = if (topics.exists(_.useAvroSchemaManager)) {
          Some(ConfigManager.getAvroSchemaManagerConfig)
        } else {
          None
        }
        KafkaTopicSettings(tinyKafkaConfig, mainTopicModel, Some(topicFieldName), topics, darwinConf)
    }
  }

  def topicNameToColumnName(s: String): String = s.replaceAllLiterally(".", "_").replaceAllLiterally("-", "_")

  /**
    * Checks that:
    * - topics are healthy as per [[areTopicsHealthy]]
    * - the topic models have the same data type
    * - the topic models have the same schema
    */
  private[wasp] def areTopicsEqualForReading(models: Seq[TopicModel]): Either[String, Unit] = {
    for {
      _ <- areTopicsHealthy(models)
      _ <- Either
            .cond(
              models.map(_.topicDataType).distinct.length == 1,
              (),
              "All topic models must have the same topic data type"
            )
      r <- Either
            .cond(models.map(_.getJsonSchema).distinct.size == 1, (), "All topic models must have the same schema")
    } yield r
  }

  private[wasp] def isTopicWritable(
      mainTopicModel: DatastoreModel,
      multiTopicModels: Seq[TopicModel],
      df: DataFrame
  ): Either[String, Unit] = {
    mainTopicModel match {
      case mt: MultiTopicModel =>
        for {
          _ <- Either.cond(multiTopicModels.nonEmpty, (), "Multi topic needs inner multiTopicModels")
          _ <- traverse(
                multiTopicModels.toList.map(t => checkTopicModelHasCoherentFields(t, df, Some(mt.topicNameField)))
              )
          _ <- areTopicsHealthy(multiTopicModels)
        } yield ()
      case t: TopicModel =>
        for {
          _ <- Either.cond(multiTopicModels.isEmpty, (), "Single topic should not have multiTopicModels")
          _ <- checkTopicModelHasCoherentFields(t, df, None)
          _ <- areTopicsHealthy(Seq(t))
        } yield ()
      case o => Left(s"${o.name} is not a topic model")
    }
  }

  def checkTopicModelHasCoherentFields(
      topic: TopicModel,
      df: DataFrame,
      topicColumn: Option[String]
  ): Either[String, List[Column]] = {
    val topicColumnName = topicColumn match {
      case Some(tColumn) =>
        catchNonFatal(df(tColumn)).left
          .map(t =>
            s"Expected column named `${tColumn}` for topic ${topic.name} " +
              s"to be used as topic, but found None: ${t.getMessage}"
          )
          .map(_ => ())
      case None => Right(())
    }
    val headerColumnName = topic.headersFieldName match {
      case Some(c) =>
        catchNonFatal(df(c)).left
          .map(t =>
            s"Expected column named `${c}` for topic ${topic.name} " +
              s"to be used as header, but found None: ${t.getMessage}"
          )
          .map(_ => ())
      case None => Right(())
    }
    val keyColumnName = topic.keyFieldName match {
      case Some(c) =>
        catchNonFatal(df(c)).left
          .map(t =>
            s"Expected column named `${c}` for topic ${topic.name} " +
              s"to be used as key, but found None: ${t.getMessage}"
          )
          .map(_ => ())
      case None => Right(())
    }

    val fieldNames = topic.topicDataType match {
      case TopicDataTypes.AVRO =>
        for {
          step1 <- checkForStructuredDataType(topic, df, topicColumn)
          step2 <- Either.cond(
                    !topic.schema.isEmpty,
                    step1,
                    s"Topic ${topic.name} datatype is avro therefore the schema should be mandatory"
                  )
        } yield step2
      case TopicDataTypes.JSON =>
        checkForStructuredDataType(topic, df, topicColumn)
      case TopicDataTypes.BINARY | TopicDataTypes.PLAINTEXT =>
        checkForPrimitiveDataType(topic, df, topicColumn)
      case dt =>
        Left(s"Unknown datatype $dt for topic ${topic.name}")
    }
    for {
      _   <- topicColumnName
      _   <- headerColumnName
      _   <- keyColumnName
      out <- fieldNames
    } yield out
  }

  private[wasp] def getAllValueFieldsFromSchema(topicModel: TopicModel): Option[List[String]] = {

    topicModel.topicDataType match {
      case TopicDataTypes.AVRO | TopicDataTypes.JSON =>
        if (topicModel.schema.isEmpty) {
          None
        } else {
          val avroSchema = new Schema.Parser().parse(topicModel.getJsonSchema)
          import scala.collection.JavaConverters._
          Some(avroSchema.getFields.asScala.map(_.name()).toList)
        }
      case _ => None
    }
  }

  private def checkForPrimitiveDataType(topic: TopicModel, dfWTopicColumn: DataFrame, topicColumn: Option[String]) = {
    topic.valueFieldsNames match {
      case Some(valueFieldsNames) if valueFieldsNames.size == 1 =>
        catchNonFatal {
          List(dfWTopicColumn(valueFieldsNames.head))
        }.left.map { t =>
          s"Unknown field $valueFieldsNames set as valueFieldsNames in topic ${topic.name}, " +
            s"original spark exception: ${t.getMessage}"
        }
      case Some(valueFieldsNames) =>
        Left(
          s"TopicModel ${topic.name} is of type ${topic.topicDataType} so valueFieldsNames " +
            s"should have only element but it has [${valueFieldsNames.mkString(", ")}]"
        )
      case None =>
        val df = topicColumn.map(dfWTopicColumn.drop).getOrElse(dfWTopicColumn)
        Either.cond(
          df.schema.fieldNames.length == 1,
          List(df(df.schema.fieldNames.head)),
          s"Dataframe for topic ${topic.name} is of type ${topic.topicDataType} therefore it " +
            s"needs to have only one column. If you need more columns to leverage headers and/or " +
            s"key features, please set valueFieldsNames accordingly. Current dataframe schema:\n" +
            df.schema.treeString
        )
    }
  }

  private def columnsOfDF(df: DataFrame) = {
    df.schema.fieldNames.map(df.apply).toList
  }

  private def parseSchema(topic: TopicModel): Either[String, Schema] = {
    catchNonFatal(new Schema.Parser().parse(topic.schema.toJson)).left
      .map(t => s"Schema of ${topic.name} cannot be parsed: ${t.getMessage}")
  }

  private def safelyProjectColumns(
      topic: TopicModel,
      fields: List[String],
      df: DataFrame
  ): Either[String, List[Column]] = {
    catchNonFatal(fields.map(df.apply)).left
      .map(t =>
        s"Fields of the schema for topic ${topic.name} " +
          s"[${fields.mkString(",")}] do not match the fields available " +
          s"in the dataframe [${df.schema.fieldNames.mkString(",")}].\n original " +
          s" spark exception: ${t.getMessage}"
      )
  }

  private def schemaAndDFmatch(
      topic: TopicModel,
      columnNames: Set[String],
      fields: List[String]
  ): Either[String, Unit] = {
    Either
      .cond(
        (columnNames -- fields.toSet).isEmpty,
        (),
        s"Dataframe for topic ${topic.name} contains more columns " +
          s"[${columnNames.mkString(",")}] than the expected by the " +
          s"schema [${fields.mkString(",")}], maybe you need to narrow the columns " +
          s"using valueFieldsNames property"
      )
  }

  private def checkForStructuredDataType(topic: TopicModel, dfWTopicColumn: DataFrame, topicColumn: Option[String]) = {
    topic.valueFieldsNames match {
      case Some(valueFieldsNames) =>
        if (topic.schema.isEmpty) {
          for {
            projectedDf <- projectValueFieldsNames(topic, dfWTopicColumn, valueFieldsNames)
          } yield columnsOfDF(projectedDf)
        } else {

          for {
            schema <- parseSchema(topic)
            fields = schema.getFields.asScala.toList.map(_.name())
            projectedDf <- projectValueFieldsNames(topic, dfWTopicColumn, valueFieldsNames)
            columnNames = projectedDf.schema.fieldNames.toSet
            columns <- safelyProjectColumns(topic, fields, projectedDf)
            _       <- schemaAndDFmatch(topic, columnNames, fields)
          } yield columns
        }
      case None =>
        val df = topicColumn.map(dfWTopicColumn.drop).getOrElse(dfWTopicColumn)
        if (topic.schema.isEmpty) {
          Right(columnsOfDF(df))
        } else {
          for {
            schema <- parseSchema(topic)
            fields      = schema.getFields.asScala.toList.map(_.name())
            columnNames = df.schema.fieldNames.toSet
            columns <- safelyProjectColumns(topic, fields, df)
            _       <- schemaAndDFmatch(topic, columnNames, fields)
          } yield columns
        }
    }
  }

  private[wasp] def topicsShareKeySchema(topics: Seq[TopicModel]): Either[String, Unit] = {
    Either.cond(
      topics.map(_.keySchema).distinct.size == 1,
      (),
      s"Cannot parse data from the following topics as a single dataframe:\n\t" +
        topics.map(_.name).mkString("\t") + "\n" +
        "It is not possible since they do not share the same key schema:\n\t" +
        topics.map(t => s"${t.name} -> ${t.keySchema}").mkString("\t")
    )
  }

  private def projectValueFieldsNames(
      topic: TopicModel,
      df: DataFrame,
      valueFieldsNames: Seq[String]
  ): Either[String, DataFrame] = {
    catchNonFatal(df.select(valueFieldsNames.map(df.apply): _*)).left.map { t =>
      s"Fields specified in valueFieldsNames of topic ${topic.name} " +
        s"[${valueFieldsNames.mkString(",")}] are not present in the df schema:\n" +
        s"${df.schema.treeString}\n" + t.getMessage
    }
  }

}

case class KafkaTopicSettings(
    tinyKafkaConfig: TinyKafkaConfig,
    mainTopicModel: DatastoreModel,
    topicFieldName: Option[String],
    topics: Seq[TopicModel],
    darwinConf: Option[Config]
) {
  val isMultiTopic: Boolean = topicFieldName.isDefined

  /**
    * For multi topic this is identical to topics field,
    * for simple topic model is a seq containing
    * the mainTopicModel
    */
  val topicsToWrite: Seq[TopicModel] =
    if (isMultiTopic) {
      topics
    } else {
      Seq(mainTopicModel.asInstanceOf[TopicModel])
    }

}
