package it.agilelab.bigdata.wasp.consumers.spark.plugins.kafka

import com.typesafe.config.Config
import it.agilelab.bigdata.wasp.consumers.spark.utils.AvroSerializerExpression
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.utils.SubjectUtils
import it.agilelab.bigdata.wasp.models.{DatastoreModel, MultiTopicModel, TopicModel}
import org.apache.avro.Schema
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression}
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

    topicFieldNameOpt match {
      case Some(topicFieldName) =>
        require(topics.size > 1, s"Got topicFieldName = $topicFieldName but only one topic to write ($topics)")
        val keyCol: Option[Column]     = keyExpression(topics, topicFieldNameOpt, df.col, darwinConf)
        val headersCol: Option[Column] = headerExpression(topics, topicFieldNameOpt)
        val topicCol: Column           = col(topicFieldName)
        val valueCol: Column           = valueExpression(topics, topicFieldNameOpt, df.schema, df.col, darwinConf)

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
        val keyCol: Option[Column]     = keyExpression(topics, topicFieldNameOpt, df.col, darwinConf)
        val headersCol: Option[Column] = headerExpression(topics, topicFieldNameOpt)
        val valueCol: Column           = valueExpression(topics, topicFieldNameOpt, df.schema, df.col, darwinConf)

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
      columnExtractor: String => Column,
      darwinConf: Option[Config]
  ): Option[Column] = {

    def valueOfKey(topicModel: TopicModel):Column = {
      topicModel.keyFieldName
        .map(keyField =>
          topicModel.topicDataType match {
            case "avro"                          => convertKeyForAvro(columnExtractor(keyField), topicModel, darwinConf)
            case "json" | "binary" | "plaintext" => convertKeyToBinary(columnExtractor(keyField))
            case unknown                         => throw new UnsupportedOperationException(s"Unknown topic data type $unknown")
          }
        )
        .getOrElse(lit(null).cast(BinaryType))
    }

    if (topics.exists(_.keyFieldName.isDefined)) {
      Some(computeFieldExpression(topics, topicFieldName, valueOfKey))
    } else {
      None
    }

  }

  private def valueExpression(
      topics: Seq[TopicModel],
      topicFieldName: Option[String],
      dfSchema: StructType,
      columnExtractor: String => Column,
      darwinConf: Option[Config]
  ): Column = {

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
    computeFieldExpression(topics, topicFieldName, valueOfValue)

  }

  private def headerExpression(
      topics: Seq[TopicModel],
      topicFieldName: Option[String]
  ): Option[Column] = {

    def valueOfHeader(head: TopicModel) = {
      head.headersFieldName.map(col).getOrElse(lit(null))
    }

    if (topics.exists(_.headersFieldName.isDefined)) {
      Some(computeFieldExpression(topics, topicFieldName, valueOfHeader))
    } else {
      None
    }

  }

  private val exceptionUdf = udf { s: String =>
    throw new Exception(s"Unknown topic name $s")
  }
  private def computeFieldExpression(
      topics: Seq[TopicModel],
      topicFieldName: Option[String],
      valueExtractor: TopicModel => Column
  ): Column = {
    topics match {
      case head :: tail if topicFieldName.isDefined =>
        val fieldName = topicFieldName.get
        tail
          .foldLeft(when(conditionOnTopicName(fieldName, head), valueExtractor(head))) { (z, x) =>
            z.when(conditionOnTopicName(fieldName, x), valueExtractor(x))
          }
          .otherwise(exceptionUdf(col(topicFieldName.get)))
      case head :: _ => valueExtractor(head)
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

    val topicsToWrite = if (topics.isEmpty) {
      List(mainTopicModel.asInstanceOf[TopicModel])
    } else {
      topics
    }

    logger.info(s"Writing with topic models: ${topicsToWrite.map(_.name).mkString(" ")}")
    if (mainTopicModel.isInstanceOf[MultiTopicModel]) {
      logger.info(s"""Topic model "${mainTopicModel.name}" is a MultiTopicModel for topics: $topics""")
    }

    logger.debug(s"Input schema:\n${stream.schema.treeString}")

    prepareDfToWrite(
      stream,
      topicFieldName,
      topicsToWrite,
      darwinConf
    )
  }

}
