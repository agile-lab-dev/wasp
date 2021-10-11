package it.agilelab.bigdata.wasp.consumers.spark.utils


import com.typesafe.config.Config
import it.agilelab.bigdata.wasp.core.utils.AvroSchemaConverters
import it.agilelab.darwin.manager.AvroSchemaManagerFactory
import org.apache.avro.Schema.Type
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{BinaryEncoder, EncoderFactory}
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, TimeZoneAwareExpression, UnaryExpression}
import org.apache.spark.sql.catalyst.util.DateTimeUtils.SQLDate
import org.apache.spark.sql.catalyst.util.{ArrayData, DateTimeUtils, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.util
import scala.collection.JavaConverters._

object AvroSerializerExpression {

  def apply(avroSchemaAsJson: Option[String],
            avroRecordName: String,
            avroNamespace: String)
           (child: Expression,
            sparkSchema: DataType): AvroSerializerExpression = {

    avroSchemaAsJson.foreach(s => checkSchemas(sparkSchema, new Schema.Parser().parse(s)))

    val maybeAvroSchemaAsJsonWrapped: Option[Either[String, Long]] =
      avroSchemaAsJson.map(x => Left(x))

    new AvroSerializerExpression(child, maybeAvroSchemaAsJsonWrapped, None, false, sparkSchema, avroRecordName, avroNamespace, None, None)
  }

  def apply(schemaManagerConfig: Config,
            avroSchema: Schema,
            avroRecordName: String,
            avroNamespace: String)
           (child: Expression,
            sparkSchema: DataType): AvroSerializerExpression = {

    val fingerprint = AvroSchemaManagerFactory.initialize(schemaManagerConfig).registerAll(Seq(avroSchema)).head._1

    checkSchemas(sparkSchema, avroSchema)

    val avroSchemaId: Option[Either[String, Long]] = Some(Right(fingerprint))

    new AvroSerializerExpression(child,
      avroSchemaId,
      Some(schemaManagerConfig),
      true,
      sparkSchema,
      avroRecordName,
      avroNamespace,
      None,
      None)
  }

  private def checkSchemas(schemaSpark: DataType, schemaAvro: Schema): Unit = {
    // flatten schemas, convert spark field list into a map
    val sparkFields: Map[String, String] = SchemaFlatteners.Spark.flattenSchema(schemaSpark, "").toMap
    // the key is the field name, the value is the field type
    val avroFields: List[(String, String)] = SchemaFlatteners.Avro.flattenSchema(schemaAvro, "")

    // iterate over the Avro field list. If any is missing from the Spark schema, or has a different type, throw an exception.
    avroFields foreach {
      case (fieldName, fieldAvroType) =>
        val maybeFieldSparkType = sparkFields.get(fieldName)
        if (maybeFieldSparkType.isEmpty) {
          // field is missing from the Spark schema
          throw new IllegalArgumentException(s"Field $fieldName in the Avro schema does not exist in the Spark schema.")
        } else if (maybeFieldSparkType.get != fieldAvroType) {
          // field has different types in the schemas
          throw new IllegalArgumentException(s"Field $fieldName has a different type in the schemas. " +
            s"Type in Avro: $fieldAvroType, type in Spark: ${maybeFieldSparkType.get}")
        }
    }
  }
}


case class AvroSerializerExpression private(child: Expression,
                                            maybeSchemaAvroJsonOrFingerprint: Option[Either[String, Long]],
                                            avroSchemaManagerConfig: Option[Config],
                                            useAvroSchemaManager: Boolean,
                                            inputSchema: DataType,
                                            structName: String,
                                            namespace: String,
                                            fieldsToWrite: Option[Set[String]],
                                            timeZoneId: Option[String]) extends UnaryExpression with ExpectsInputTypes with TimeZoneAwareExpression {


  @transient private lazy val schemaManager = avroSchemaManagerConfig.map(AvroSchemaManagerFactory.initialize)

  @transient private lazy val externalSchema: Option[Schema] = maybeSchemaAvroJsonOrFingerprint.map {
    case Left(json) => new Schema.Parser().parse(json)
    case Right(fingerprint) =>
      schemaManager.get.getSchema(fingerprint) match {
        case None => throw new IllegalStateException(s"Schema with fingerprint [$fingerprint] was not found in schema registry")
        case Some(schema) => schema
      }
  }

  @transient private lazy val converter =
    createConverterToAvro(inputSchema, structName, namespace, fieldsToWrite, externalSchema)

  @transient private lazy val actualSchema: Schema = externalSchema.getOrElse {
    val builder = SchemaBuilder.record(structName).namespace(namespace)

    inputSchema match {
      case struct: StructType =>
        AvroSchemaConverters.convertStructToAvro(struct, builder, namespace)
      case otherwise =>
        AvroSchemaConverters.convertTypeToAvro(otherwise, SchemaBuilder.builder(), "", recordNamespace = namespace)
    }
  }

  // convenient method for accessing the schema in codegen
  def getActualSchema: Schema = actualSchema

  private val schemaId = {
    maybeSchemaAvroJsonOrFingerprint match {
      case Some(Right(fingerprint)) if useAvroSchemaManager => fingerprint
      case _ if useAvroSchemaManager => throw new IllegalStateException("We should have a fingerprint because we are using the schema registry")
      case _ => -1L // we will not access schema id in this case, take care.
    }
  }

  override def inputTypes = {
    inputSchema match {
      case s: StructType =>
        Seq(StructType)
      case d: DataType =>
        Seq(d)
    }
  }

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression = copy(timeZoneId = Some(timeZoneId))

  override def nullable: Boolean = child.nullable

  def serializeInternalRow(row: AnyRef,
                           output: ByteArrayOutputStream,
                           encoder: BinaryEncoder,
                           writer: GenericDatumWriter[AnyRef]): Array[Byte] = {
    if (useAvroSchemaManager) {
      schemaManager.get.writeHeaderToStream(output, schemaId)
    }
    val value = converter(row).asInstanceOf[AnyRef]
    writer.write(value, encoder)
    encoder.flush()
    output.toByteArray
  }

  override protected def nullSafeEval(input: Any): Any = {
    val output = new ByteArrayOutputStream()
    val writer = new GenericDatumWriter[AnyRef](actualSchema)
    serializeInternalRow(input.asInstanceOf[AnyRef], output,
      EncoderFactory.get().binaryEncoder(output, null),
      writer)


  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // come se venisse fatto all'inizio di una mapPartitions
    val bufferClassName = classOf[ByteArrayOutputStream].getName
    val bufferName = ctx.addMutableState(
      bufferClassName,
      "buffer",
      name => s"$name = new $bufferClassName();")

    val encoderClassName = classOf[BinaryEncoder].getName
    val encoderFactoryClassName = classOf[EncoderFactory].getName
    val encoderName = ctx.addMutableState(
      encoderClassName,
      "encoder",
      encoder => s"""$encoder = $encoderFactoryClassName.get().binaryEncoder($bufferName, null);"""
    )

    val avroConverterExpression =
      ctx.addReferenceObj("avroConverterExpr", this, this.getClass.getName)

    val genericWriterClassName = classOf[GenericDatumWriter[GenericRecord]].getName
    val genericRecordClassName = classOf[GenericRecord].getName
    val genericWriterName = ctx.addMutableState(
      genericWriterClassName,
      "genericWriter",
      name => s"""$name = new $genericWriterClassName<$genericRecordClassName>($avroConverterExpression.getActualSchema());""")

    val childCode = child.genCode(ctx)
    val newEncoderName = ctx.freshName("newEncoder")
    ev.copy(code =
      code"""
            |${childCode.code}
            |byte[] ${ev.value} = null;
            |if (!${childCode.isNull}) {
            |  $bufferName.reset();
            |  $encoderClassName $newEncoderName = $encoderFactoryClassName.get().binaryEncoder($bufferName, $encoderName);
            |  ${ev.value} = $avroConverterExpression.serializeInternalRow(
            |    ${childCode.value}, $bufferName, $newEncoderName, $genericWriterName);
            |}
       """.stripMargin, isNull = childCode.isNull)
  }

  override def dataType: DataType = BinaryType

  private def createConverterToAvro(
                                     sparkSchema: DataType,
                                     structName: String,
                                     recordNamespace: String,
                                     fieldsToWrite: Option[Set[String]],
                                     externalSchema: Option[Schema]): Any => Any = {
    sparkSchema match {
      case BinaryType => (item: Any) =>
        item match {
          case null => null
          case bytes: Array[Byte] => ByteBuffer.wrap(bytes)
        }
      case StringType => {
        case null => null
        case u: UTF8String => u.toString
        case item => item // never here, I hope
      }
      case ByteType | ShortType | IntegerType | LongType |
           FloatType | DoubleType | BooleanType => identity
      case TimestampType => (item: Any) =>
        item.asInstanceOf[Long] / 1000

      case _: DecimalType => (item: Any) => if (item == null) null else item.toString
      // identity because we return the long as is
      // case TimestampType => (item: Any) =>
      //  if (item == null) null else item.asInstanceOf[Long]
      case DateType => (item: Any) =>
        if (item == null) {
          null
        } else {
          DateTimeUtils.daysToMillis(item.asInstanceOf[SQLDate], timeZone)
        }
      case ArrayType(elementType, _) =>
        val extractElemTypeFromUnion = externalSchema.map(s => eventualSubSchemaFromUnionWithNull(s))
        val elementConverter = createConverterToAvro(elementType, structName, recordNamespace, None, extractElemTypeFromUnion.map(s => s.getElementType))
        (item: Any) => {
          if (item == null) {
            null
          } else {
            val sourceArray = item.asInstanceOf[ArrayData]
            val sourceArraySize = sourceArray.numElements()
            val targetArray = new util.ArrayList[Any](sourceArraySize)
            var idx = 0
            while (idx < sourceArraySize) {
              targetArray.add(idx, elementConverter(sourceArray.get(idx, elementType)))
              idx += 1
            }
            targetArray
          }
        }
      case MapType(StringType, valueType, _) =>
        val extractElemTypeFromUnion = externalSchema.map(s => eventualSubSchemaFromUnionWithNull(s))
        val valueConverter = createConverterToAvro(valueType, structName, recordNamespace, None, extractElemTypeFromUnion.map(s => s.getValueType))
        (item: Any) => {
          if (item == null) {
            null
          } else {
            val javaMap = new util.HashMap[String, Any]()
            val keys = item.asInstanceOf[MapData].keyArray()
            val values = item.asInstanceOf[MapData].valueArray()
            var i = 0
            while (i < keys.numElements()) {
              javaMap.put(keys.getUTF8String(i).toString, valueConverter(values.get(i, valueType)))
              i += 1
            }
            javaMap
          }
        }
      case structType: StructType =>
        val builder = SchemaBuilder.record(structName).namespace(recordNamespace)
        val schema: Schema = externalSchema.map(eventualSubSchemaFromUnionWithNull).getOrElse(
          AvroSchemaConverters.convertStructToAvro(structType, builder, recordNamespace)
        )

        val fieldConverters = structType.fields.filter(f => {
          if (fieldsToWrite.isDefined) {
            fieldsToWrite.get.contains(f.name)
          } else {
            true
          }
        }).map { field =>
          val maybeSubfieldSchema = Option(schema.getField(field.name)).map(_.schema())

          if (maybeSubfieldSchema.isEmpty) {
            val message =
              s"""spark schema contains a field [${field.name}] of type [${field.dataType}]
                 |no corresponding field is present in the avro schema, maybe you forgot to
                 |drop kafkaMetadata column or to configure 'valueFieldsNames' in TopicModel
                 |to specify which columns should be treated as the value
                 |""".stripMargin

            throw new IllegalArgumentException(message)
          }

          createConverterToAvro(field.dataType, field.name, recordNamespace, None, maybeSubfieldSchema)
        }
        (item: Any) => {
          if (item == null) {
            null
          } else {
            val record = new Record(schema)
            val convertersIterator = fieldConverters.iterator
            val fields = structType.fields
            var i = 0

            while (convertersIterator.hasNext) {
              val converter = convertersIterator.next()
              record.put(fields(i).name, converter(item.asInstanceOf[InternalRow].get(i, fields(i).dataType)))
              i += 1
            }
            record
          }
        }
    }
  }

  private def eventualSubSchemaFromUnionWithNull(s: Schema): Schema = {
    if (s.getType == Type.UNION) {
      val otherType = s.getTypes.asScala.filter(subS => subS.getType != Type.NULL)
      if (otherType.size != 1) {
        throw new IllegalArgumentException(
          s"Avro sub-schema ${s.getName} has UnionSchema which is not a simple NullSchema + primitive schema.")
      }
      otherType.head
    } else {
      s
    }
  }
}

