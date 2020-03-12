package it.agilelab.bigdata.wasp.consumers.spark.plugins.kafka

import it.agilelab.bigdata.wasp.consumers.spark.utils.AvroSerializerExpression
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.WaspSystem.??
import it.agilelab.bigdata.wasp.core.bl.TopicBL
import it.agilelab.bigdata.wasp.core.datastores.TopicCategory
import it.agilelab.bigdata.wasp.core.kafka.CheckOrCreateTopic
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.{DatastoreModel, MultiTopicModel, TopicModel}
import it.agilelab.bigdata.wasp.core.utils.{ConfigManager, StringToByteArrayUtil}
import org.apache.avro.Schema
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.functions.{col, struct, udf}
import org.apache.spark.sql.types.{BinaryType, StringType, StructType}
import org.apache.spark.sql.{Column, DataFrame}

object KafkaWriters extends Logging {
  private[kafka] def convertDfForBinary(keyFieldName: Option[String],
                                        headersFieldName: Option[String],
                                        topicFieldName: Option[String],
                                        valueFieldsNames: Option[Seq[String]],
                                        df: DataFrame,
                                        prototypeTopicModel: TopicModel) = {
    // there must be exactly one value field name and it must be a column of type binary
    require(valueFieldsNames.isDefined && valueFieldsNames.get.size == 1,
      "Exactly one value field name must be defined for binary topic data type but zero or more than one were " +
        s"specified; value field names: ${valueFieldsNames.get.mkString("\"", "\", \"", "\"")}")
    val valueFieldName = valueFieldsNames.get.head
    val maybeValueColumn = df.schema.find(_.name == valueFieldName)
    require(maybeValueColumn.isDefined,
      s"""The specified value field name "$valueFieldName" does not match any column; columns in schema: """ +
        s"""${df.schema.map(_.name).mkString("[", "], [", "]")}""")
    val valueColumn = maybeValueColumn.get
    val valueColumnDataType = valueColumn.dataType
    require(valueColumnDataType == BinaryType,
      s"""The specified value field name "$valueFieldName" matches a column with a type that is not binary; """ +
        s"incompatible type $valueColumnDataType found")

    // generate select expressions to rename metadata and data columns
    val selectExpressions =
      keyFieldName.map(kfn => s"CAST($kfn AS binary) key").toList ++
        headersFieldName.map(hfn => s"$hfn AS headers").toList ++
        topicFieldName.map(tfn => s"$tfn AS topic").toList :+
        s"$valueFieldName AS value"

    logger.debug(s"Generated select expressions: ${selectExpressions.mkString("[", "], [", "]")}")

    // convert input
    df.selectExpr(selectExpressions: _*)
  }

  private[kafka] def convertDfForAvro(keyFieldName: Option[String],
                                      headersFieldName: Option[String],
                                      topicFieldName: Option[String],
                                      valueFieldsNames: Option[Seq[String]],
                                      df: DataFrame,
                                      prototypeTopicModel: TopicModel) = {
    val columnsInValues = valueFieldsNames.getOrElse(df.columns.toSeq)
    // generate a schema and avro converter for the values
    val valueSchema = StructType(
      columnsInValues.map(df.schema.apply)
    )
    val darwinConf = if (prototypeTopicModel.useAvroSchemaManager) {
      Some(ConfigManager.getAvroSchemaManagerConfig)
    } else {
      None
    }
    val exprToConvertToAvro = struct(columnsInValues.map(col): _*)

    val avroRecordName = prototypeTopicModel.name
    // TODO use sensible namespace instead of wasp
    val avroRecordNamespace = "wasp"

    val rowToAvroExprFactory: (Expression, StructType) => AvroSerializerExpression = if (prototypeTopicModel.useAvroSchemaManager) {
      val avroSchema = new Schema.Parser().parse(prototypeTopicModel.getJsonSchema)
      AvroSerializerExpression(darwinConf.get, avroSchema, avroRecordName, avroRecordNamespace)
    } else {
      AvroSerializerExpression(Some(prototypeTopicModel.getJsonSchema), avroRecordName, avroRecordNamespace)
    }

    val rowToAvroExpr = rowToAvroExprFactory(exprToConvertToAvro.expr, valueSchema)

    val metadataCols = (keyFieldName.map(kfn => col(kfn).cast(BinaryType).as("key")) ++
      headersFieldName.map(col(_).as("headers")) ++
      topicFieldName.map(col(_).as("topic"))).toSeq

    val processedDf = df.select(metadataCols ++ Seq(new Column(rowToAvroExpr).as("value")): _*)

    logger.debug(s"Actual final schema:\n${processedDf.schema.treeString}")

    processedDf
  }

  private[kafka] def convertDfForJson(keyFieldName: Option[String],
                                      headersFieldName: Option[String],
                                      topicFieldName: Option[String],
                                      valueFieldsNames: Option[Seq[String]],
                                      df: DataFrame,
                                      prototypeTopicModel: TopicModel) = {
    // generate select expressions to rename metadata columns and convert everything to json
    val valueSelectExpression = valueFieldsNames.map(vfn => vfn).getOrElse(Seq("*")).mkString(", ")
    val selectExpressions =
      keyFieldName.map(kfn => s"CAST($kfn AS binary) key").toList ++
        headersFieldName.map(hfn => s"$hfn AS headers").toList ++
        topicFieldName.map(tfn => s"$tfn AS topic").toList :+
        s"to_json(struct($valueSelectExpression)) AS value"

    logger.debug(s"Generated select expressions: ${selectExpressions.mkString("[", "], [", "]")}")

    // TODO check that json produced matches schema

    // convert input
    df.selectExpr(selectExpressions: _*)
  }

  private[kafka] def convertDfForPlaintext(keyFieldName: Option[String],
                                           headersFieldName: Option[String],
                                           topicFieldName: Option[String],
                                           valueFieldsNames: Option[Seq[String]],
                                           df: DataFrame,
                                           prototypeTopicModel: TopicModel) = {
    // there must be exactly one value field name and it must be a column of type string
    require(valueFieldsNames.isDefined && valueFieldsNames.get.size == 1,
      "Exactly one value field name must be defined for plaintext topic data type but zero or more than one " +
        s"were specified; value field names: ${valueFieldsNames.get.mkString("\"", "\", \"", "\"")}")
    val valueFieldName = valueFieldsNames.get.head
    val maybeValueColumn = df.schema.find(_.name == valueFieldName)
    require(maybeValueColumn.isDefined,
      s"""The specified value field name "$valueFieldName" does not match any column; columns in schema: """ +
        s"""${df.schema.map(_.name).mkString("[", "], [", "]")}""")
    val valueColumn = maybeValueColumn.get
    val valueColumnDataType = valueColumn.dataType
    require(valueColumnDataType == StringType,
      s"""The specified value field name "$valueFieldName" matches a column with a type that is not string; """ +
        s"incompatible type $valueColumnDataType found")

    // generate select expressions to rename metadata and data columns
    val selectExpressions =
      keyFieldName.map(kfn => s"CAST($kfn AS binary) key").toList ++
        headersFieldName.map(hfn => s"$hfn AS headers").toList ++
        topicFieldName.map(tfn => s"$tfn AS topic").toList :+
        s"$valueFieldName AS value_string"

    logger.debug(s"Generated select expressions: ${selectExpressions.mkString("[", "], [", "]")}")

    // prepare the udf
    val stringToByteArray: String => Array[Byte] = StringToByteArrayUtil.stringToByteArray
    val stringToByteArrayUDF = udf(stringToByteArray)

    // convert input
    df
      .selectExpr(selectExpressions: _*)
      .withColumn("value", stringToByteArrayUDF(col("value_string")))
      .drop("value_string")
  }

  private[kafka] def addTopicNameCheckIfNeeded(topicFieldName: Option[String], topics: Seq[TopicModel], df: DataFrame) = {
    if (topicFieldName.isEmpty) {
      // no checks to be done as there is no per-row topic selection
      df
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
      df.withColumn("topic", checkTopicNameUdf(col("topic")))
    }
  }


  private[kafka] def askToCheckOrCreateTopics(topics: Seq[TopicModel]): Unit = {
    logger.info(s"Creating topics $topics")

    topics.foreach(topic =>
      if (! ??[Boolean](WaspSystem.kafkaAdminActor,
        CheckOrCreateTopic(topic.name,
          topic.partitions,
          topic.replicas)))
        throw new Exception(s"""Error creating topic "${topic.name}"""")
    )
  }

  private[kafka] def retrieveTopicFieldNameAndTopicModels(topicOpt: Option[DatastoreModel[TopicCategory]],
                                                          topicBL: TopicBL,
                                                          topicDatastoreModelName: String) = {
    topicOpt match {
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
  }

  private[kafka] def prepareDfToWrite(df: DataFrame,
                                      topicFieldName: Option[String],
                                      topics: Seq[TopicModel],
                                      prototypeTopicModel: TopicModel,
                                      keyFieldName: Option[String],
                                      headersFieldName: Option[String],
                                      valueFieldsNames: Option[Seq[String]]) = {
    val convertedDF = prototypeTopicModel.topicDataType match {
      case "avro" =>
        convertDfForAvro(keyFieldName,
          headersFieldName,
          topicFieldName,
          valueFieldsNames,
          df,
          prototypeTopicModel)
      case "json" =>
        convertDfForJson(keyFieldName,
          headersFieldName,
          topicFieldName,
          valueFieldsNames,
          df,
          prototypeTopicModel)
      case "plaintext" =>
        convertDfForPlaintext(keyFieldName,
          headersFieldName,
          topicFieldName,
          valueFieldsNames,
          df,
          prototypeTopicModel)
      case "binary" =>
        convertDfForBinary(keyFieldName,
          headersFieldName,
          topicFieldName,
          valueFieldsNames,
          df,
          prototypeTopicModel)

      case topicDataType =>
        throw new UnsupportedOperationException(s"Unknown topic data type $topicDataType")
    }

    val finalDf = addTopicNameCheckIfNeeded(topicFieldName, topics, convertedDF)
    finalDf
  }
}