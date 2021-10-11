package it.agilelab.bigdata.wasp.consumers.spark.plugins.kafka

import com.typesafe.config.Config
import it.agilelab.bigdata.wasp.consumers.spark.utils.AvroSerializerExpression
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.utils.SubjectUtils
import it.agilelab.bigdata.wasp.models.{DatastoreModel, MultiTopicModel, TopicModel}
import org.apache.avro.Schema
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BinaryType, DataType, StringType, StructType}
import org.apache.spark.sql.{Column, DataFrame}

object KafkaWriters extends Logging {

  private[kafka] def addTopicNameCheckIfNeeded(
      topicFieldName: Option[String],
      topics: Seq[TopicModel],
      df: DataFrame
  ) = {
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

  private[kafka] def prepareDfToWrite(
      df: DataFrame,
      topicFieldNameOpt: Option[String],
      topics: Seq[TopicModel],
      darwinConf: Option[Config]
  ) = {

    val throwException = udf { s: String =>
      throw new Exception(s"Unknown topic name $s")
    }

    topicFieldNameOpt match {
      case Some(topicFieldName) =>
        require(topics.size > 1, s"Got topicFieldName = $topicFieldName but only one topic to write ($topics)")
        val keyCol: Option[Column]     = keyExpression(topics, topicFieldNameOpt, throwException, df.col, darwinConf)
        val headersCol: Option[Column] = headerExpression(topics, topicFieldNameOpt, throwException)
        val topicCol: Column           = col(topicFieldName)
        val valueCol: Column           = valueExpression(topics, topicFieldNameOpt, df.schema, df.col, throwException, darwinConf)

        val columns =
          (keyCol.map(_.as("key")) ++
            headersCol.map(_.as("headers")) ++
            Seq(topicCol.as("topic"), valueCol.as("value"))).toSeq

        df.select(columns: _*)

      case None =>
        require(
          topics.size == 1,
          "More than one topic to write specified but there's no column containing the topics' name."
        )
        val keyCol: Option[Column]     = keyExpression(topics, topicFieldNameOpt, throwException, df.col, darwinConf)
        val headersCol: Option[Column] = headerExpression(topics, topicFieldNameOpt, throwException)
        val valueCol: Column           = valueExpression(topics, topicFieldNameOpt, df.schema, df.col, throwException, darwinConf)

        val columns =
          (keyCol.map(_.as("key")) ++
            headersCol.map(_.as("headers")) ++
            Seq(valueCol.as("value"))).toSeq
        df.select(columns: _*)

    }

  }

  private def keyExpression(
      topics: Seq[TopicModel],
      topicFieldName: Option[String],
      exceptionUdf: UserDefinedFunction,
      columnExtractor: String => Column,
      darwinConf: Option[Config]
  ) = {

    def valueOfKey(topicModel: TopicModel): Column = {
      val keyField = topicModel.keyFieldName.get
      topicModel.topicDataType match {
        case "avro" => convertKeyForAvro(columnExtractor(keyField), topicModel, darwinConf)
        case dataType if dataType == "json" || dataType == "binary" || dataType == "plaintext" =>
          convertKeyToBinary(columnExtractor(keyField))
        case unknown => throw new UnsupportedOperationException(s"Unknown topic data type $unknown")
      }
    }

    if (topics.exists(_.keyFieldName.isDefined)) {

      if (topicFieldName.isDefined) {
        val head = topics.head
        val tail = topics.tail

        Some(
          tail
            .foldLeft(when(conditionOnTopicName(topicFieldName.get, head), valueOfKey(head))) { (z, x) =>
              z.when(conditionOnTopicName(topicFieldName.get, x), valueOfKey(x))
            }
            .otherwise(exceptionUdf(col(topicFieldName.get)))
        )
      } else {
        Some(valueOfKey(topics.head))
      }

    } else {
      None
    }
  }

  private def valueExpression(
      topics: Seq[TopicModel],
      topicFieldName: Option[String],
      dfSchema: StructType,
      columnExtractor: String => Column,
      exceptionUdf: UserDefinedFunction,
      darwinConf: Option[Config]
  ) = {

    def valueOfValue(topicModel: TopicModel): Column = {
      val columnsInValues = topicModel.valueFieldsNames match {
        case Some(values) => values
        case None =>
          TopicModelUtils.getAllValueFieldsFromSchema(topicModel).getOrElse(dfSchema.fieldNames.toList)
      }
      topicModel.topicDataType match {
        case "avro"      => convertValueForAvro(columnsInValues, topicModel, dfSchema, columnExtractor, darwinConf)
        case "json"      => convertValueForJson(columnsInValues)
        case "plaintext" => convertValueForPlainText(columnsInValues, dfSchema)
        case "binary"    => convertValueForBinary(columnsInValues, dfSchema)
        case unknown     => throw new UnsupportedOperationException(s"Unknown topic data type $unknown")
      }
    }

    if (topicFieldName.isDefined) {
      val head = topics.head
      val tail = topics.tail

      tail
        .foldLeft(when(conditionOnTopicName(topicFieldName.get, head), valueOfValue(head))) {
          (z: Column, x: TopicModel) =>
            z.when(conditionOnTopicName(topicFieldName.get, x), valueOfValue(x))
        }
        .otherwise(exceptionUdf(col(topicFieldName.get)))

    } else {
      valueOfValue(topics.head)
    }

  }

  private def headerExpression(
      topics: Seq[TopicModel],
      topicFieldName: Option[String],
      exceptionUdf: UserDefinedFunction
  ) = {

    def valueOfHeader(head: TopicModel) = {
      head.headersFieldName.map(col).getOrElse(lit(null))
    }

    if (topics.exists(_.headersFieldName.isDefined)) {

      if (topicFieldName.isDefined) {
        val head = topics.head
        val tail = topics.tail
        Some(
          tail
            .foldLeft(when(conditionOnTopicName(topicFieldName.get, head), valueOfHeader(head))) {
              (z: Column, x: TopicModel) =>
                z.when(conditionOnTopicName(topicFieldName.get, x), valueOfHeader(x))
            }
            .otherwise(exceptionUdf(col(topicFieldName.get)))
        )
      } else {
        Some(valueOfHeader(topics.head))
      }
    } else {
      None
    }

  }

  private def conditionOnTopicName(topicFieldName: String, head: TopicModel) = {
    col(topicFieldName).equalTo(head.name)
  }

  private def convertKeyForAvro(keyColumn: Column, topicModel: TopicModel, darwinConf: Option[Config]) = {

    if (topicModel.keySchema.isDefined) {
      val exprToConvertToAvro = keyColumn
      val keySchema           = exprToConvertToAvro.expr.dataType

      val avroRecordName = topicModel.name + "key"
      // TODO use sensible namespace instead of wasp
      val avroRecordNamespace = "wasp"

      val rowToAvroExprFactory: (Expression, DataType) => AvroSerializerExpression =
        if (topicModel.useAvroSchemaManager) {
          val avroSchema = new Schema.Parser().parse(topicModel.keySchema.get)

          val updatedSchema = SubjectUtils.attachSubjectToSchema(topicModel, avroSchema, isKey = true)

          AvroSerializerExpression(darwinConf.get, updatedSchema, avroRecordName, avroRecordNamespace)
        } else {
          AvroSerializerExpression(Some(topicModel.keySchema.get), avroRecordName, avroRecordNamespace)
        }

      val rowToAvroExpr = rowToAvroExprFactory(exprToConvertToAvro.expr, keySchema)
      new Column(rowToAvroExpr).as("key")
    } else {
      if (Cast.canCast(keyColumn.expr.dataType, BinaryType)) {
        keyColumn.cast(BinaryType).as("key")
      } else
        throw new Exception(
          s"Cannot serialize key for Kafka topic because column ${keyColumn.toString()} cannot " +
            s"be cast to Binary. If you want to serialize it in Avro format, set TopicModel.keySchema accordingly."
        )
    }

  }

  private def convertKeyToBinary(keyColumn: Column) = {
    if (Cast.canCast(keyColumn.expr.dataType, BinaryType)) {
      keyColumn.cast(BinaryType).as("key")
    } else
      throw new Exception(
        s"Cannot serialize key for Kafka topic because column ${keyColumn.toString()} cannot " +
          s"be cast to Binary."
      )
  }

  private def convertValueForJson(columnsInValues: Seq[String]) = {

    val columnExpr = to_json(struct(columnsInValues.map(col): _*)).cast(BinaryType).as("value")

    logger.debug(s"Generated select expression: ${columnExpr.expr.toString()}")
    columnExpr
  }

  private def convertValueForPlainText(columnsInValues: Seq[String], schema: StructType) = {

    require(
      columnsInValues.size == 1,
      "Exactly one value field name must be defined for plaintext topic data type but zero or more than one " +
        s"were specified; value field names: ${columnsInValues.mkString("\"", "\", \"", "\"")}"
    )
    val valueFieldName = columnsInValues.head

    val maybeValueColumn = schema.find(_.name == valueFieldName)
    require(
      maybeValueColumn.isDefined,
      s"""The specified value field name "$valueFieldName" does not match any column; columns in schema: """ +
        s"""${schema.map(_.name).mkString("[", "], [", "]")}"""
    )

    val valueColumn         = maybeValueColumn.get
    val valueColumnDataType = valueColumn.dataType
    require(
      Cast.canCast(valueColumnDataType, StringType),
      s"""The specified value field name "$valueFieldName" matches a column with a type that is not string; """ +
        s"incompatible type $valueColumnDataType found"
    )

    val columnExpr = col(valueFieldName).cast(StringType).cast(BinaryType).as("value")

    logger.debug(s"Generated select expression: ${columnExpr.expr.toString()}")
    columnExpr
  }

  def convertValueForBinary(columnsInValues: Seq[String], schema: StructType): Column = {
    require(
      columnsInValues.size == 1,
      "Exactly one value field name must be defined for binary topic data type but zero or more than one were " +
        s"specified; value field names: ${columnsInValues.mkString("\"", "\", \"", "\"")}"
    )
    val valueFieldName   = columnsInValues.head
    val maybeValueColumn = schema.find(_.name == valueFieldName)
    require(
      maybeValueColumn.isDefined,
      s"""The specified value field name "$valueFieldName" does not match any column; columns in schema: """ +
        s"""${schema.map(_.name).mkString("[", "], [", "]")}"""
    )
    val valueColumn         = maybeValueColumn.get
    val valueColumnDataType = valueColumn.dataType
    require(
      valueColumnDataType == BinaryType,
      s"""The specified value field name "$valueFieldName" matches a column with a type that is not binary; """ +
        s"incompatible type $valueColumnDataType found"
    )

    val expression = s"$valueFieldName AS value"
    logger.debug(s"Generated select expressions: ${expression}")

    expr(expression)
  }

  private def convertValueForAvro(
      columnsInValues: Seq[String],
      topicModel: TopicModel,
      schema: StructType,
      columnExtractor: String => Column,
      darwinConf: Option[Config]
  ): Column = {

    val exprToConvertToAvro = struct(columnsInValues.map(columnExtractor): _*).expr
    val valueSchema         = exprToConvertToAvro.dataType.asInstanceOf[StructType] // this is safe

    val avroRecordName = topicModel.name
    // TODO use sensible namespace instead of wasp
    val avroRecordNamespace = "wasp"

    val rowToAvroExprFactory: (Expression, StructType) => AvroSerializerExpression =
      if (topicModel.useAvroSchemaManager) {
        val avroSchema    = new Schema.Parser().parse(topicModel.getJsonSchema)
        val updatedSchema = SubjectUtils.attachSubjectToSchema(topicModel, avroSchema, false)
        AvroSerializerExpression(darwinConf.get, updatedSchema, avroRecordName, avroRecordNamespace)
      } else {
        AvroSerializerExpression(Some(topicModel.getJsonSchema), avroRecordName, avroRecordNamespace)
      }

    val rowToAvroExpr = rowToAvroExprFactory(exprToConvertToAvro, valueSchema)
    new Column(rowToAvroExpr)
  }

  def convertDataframe(
      stream: DataFrame,
      topicFieldName: Option[String],
      topics: Seq[TopicModel],
      mainTopicModel: DatastoreModel,
      darwinConf: Option[Config]
  ): DataFrame = {

    TopicModelUtils
      .isTopicWritable(mainTopicModel, topics, stream)
      .fold(
        s => throw new IllegalArgumentException(s),
        _ => ()
      )

    logger.info(s"Writing with topic models: ${topics.map(_.name).mkString(" ")}")
    if (mainTopicModel.isInstanceOf[MultiTopicModel]) {
      logger.info(s"""Topic model "${mainTopicModel.name}" is a MultiTopicModel for topics: $topics""")
    }

    logger.debug(s"Input schema:\n${stream.schema.treeString}")

    val topicsToWrite = if (topics.isEmpty) {
      List(mainTopicModel.asInstanceOf[TopicModel])
    } else {
      topics
    }

    prepareDfToWrite(
      stream,
      topicFieldName,
      topicsToWrite,
      darwinConf
    )
  }

}
