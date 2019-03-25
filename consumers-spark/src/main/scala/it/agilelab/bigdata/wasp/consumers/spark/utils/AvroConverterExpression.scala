package it.agilelab.bigdata.wasp.consumers.spark.utils


import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.util

import com.typesafe.config.Config
import it.agilelab.darwin.manager.AvroSchemaManagerFactory
import it.agilelab.darwin.manager.util.AvroSingleObjectEncodingUtils
import org.apache.avro.Schema.Type
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{BinaryEncoder, EncoderFactory}
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow, TimeZoneAwareExpression}
import org.apache.spark.sql.catalyst.util.DateTimeUtils.SQLDate
import org.apache.spark.sql.catalyst.util.{ArrayData, DateTimeUtils, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._


object AvroConverterExpression {

  def apply(avroSchemaAsJson: Option[String],
            avroRecordName: String,
            avroNamespace: String)
           (children: Seq[Expression],
            sparkSchema: StructType): AvroConverterExpression = {

    avroSchemaAsJson.foreach(s => RowToAvro.checkSchemas(sparkSchema, s))

    val maybeAvroSchemaAsJsonWrapped: Option[Either[String, Long]] =
      avroSchemaAsJson.map(x => Left(x))

    new AvroConverterExpression(children, maybeAvroSchemaAsJsonWrapped, None, false, sparkSchema, avroRecordName, avroNamespace, None, None)
  }

  def apply(schemaManagerConfig: Config,
            avroSchema: Schema,
            avroRecordName: String,
            avroNamespace: String)
           (children: Seq[Expression],
            sparkSchema: StructType): AvroConverterExpression = {

    val fingerprint = AvroSchemaManagerFactory.getInstance(schemaManagerConfig).registerAll(Seq(avroSchema)).head._1

    RowToAvro.checkSchemas(sparkSchema, avroSchema)

    val avroSchemaId: Option[Either[String, Long]] = Some(Right(fingerprint))

    new AvroConverterExpression(children,
      avroSchemaId,
      Some(schemaManagerConfig),
      true,
      sparkSchema,
      avroRecordName,
      avroNamespace,
      None,
      None)
  }
}


case class AvroConverterExpression private(children: Seq[Expression],
                                           maybeSchemaAvroJsonOrFingerprint: Option[Either[String, Long]],
                                           avroSchemaManagerConfig: Option[Config],
                                           useAvroSchemaManager: Boolean,
                                           inputSchema: StructType,
                                           structName: String,
                                           namespace: String,
                                           fieldsToWrite: Option[Set[String]],
                                           timeZoneId: Option[String]) extends Expression with TimeZoneAwareExpression {


  @transient private lazy val externalSchema: Option[Schema] = maybeSchemaAvroJsonOrFingerprint.map {
    case Left(json) => new Schema.Parser().parse(json)
    case Right(fingerprint) =>
      AvroSchemaManagerFactory.getInstance(avroSchemaManagerConfig.get).getSchema(fingerprint) match {
        case None => throw new IllegalStateException(s"Schema with fingerprint [$fingerprint] was not found in schema registry")
        case Some(schema) => schema
      }
  }

  @transient private lazy val converter =
    createConverterToAvro(inputSchema, structName, namespace, fieldsToWrite, externalSchema)

  @transient private lazy val actualSchema: Schema = externalSchema.getOrElse {
    val builder = SchemaBuilder.record(structName).namespace(namespace)
    SchemaConverters.convertStructToAvro(inputSchema, builder, namespace)
  }

  private val schemaId = {
    maybeSchemaAvroJsonOrFingerprint match {
      case Some(Right(fingerprint)) if useAvroSchemaManager => fingerprint
      case _ if useAvroSchemaManager => throw new IllegalStateException("We should have a fingerprint because we are using the schema registry")
      case _ => -1L // we will not access schema id in this case, take care.
    }
  }


  override def checkInputDataTypes(): TypeCheckResult = {
    if (children == Nil) {
      TypeCheckResult.TypeCheckFailure("input to function AvroConverterExpression cannot be empty")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression = copy(timeZoneId = Some(timeZoneId))

  override def nullable: Boolean = false

  def serializeInternalRow(row: InternalRow,
                           output: ByteArrayOutputStream,
                           encoder: BinaryEncoder): Array[Byte] = {
    if (useAvroSchemaManager) {
      AvroSingleObjectEncodingUtils.writeHeaderToStream(output, schemaId)
    }
    val value: GenericRecord = converter(row).asInstanceOf[GenericRecord]
    val writer = new GenericDatumWriter[GenericRecord](actualSchema)
    writer.write(value, encoder)
    encoder.flush()
    output.toByteArray
  }

  override def eval(input: InternalRow): Any = {
    val output = new ByteArrayOutputStream()
    serializeInternalRow(InternalRow.fromSeq(children.map(_.eval(input))), output,
      EncoderFactory.get().binaryEncoder(output, null))
  }


  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // come se venisse fatto all'inizio di una mapPartitions
    val bufferClassName = classOf[ByteArrayOutputStream].getName
    val bufferName = ctx.freshName("buffer")
    ctx.addMutableState(
      bufferClassName,
      bufferName,
      s"$bufferName = new $bufferClassName();")

    val encoderClassName = classOf[BinaryEncoder].getName()
    val encoderFactoryClassName = classOf[EncoderFactory].getName()
    val encoderName = ctx.freshName("encoder")
    ctx.addMutableState(
      encoderClassName,
      encoderName,
      s"""$encoderName = $encoderFactoryClassName.get().binaryEncoder($bufferName, null);"""
    )
    val avroConverterExprName = ctx.freshName("avroConverterExpr")
    val rowClass = classOf[GenericInternalRow].getName
    val values = ctx.freshName("values")

    val avroConverterExpression =
      ctx.addReferenceObj(avroConverterExprName, this, this.getClass.getName)
    val valCodes = children.zipWithIndex.map { case (e, i) =>
      val eval = e.genCode(ctx)
      s"""
         |${eval.code}
         |if (${eval.isNull}) {
         |  $values[$i] = null;
         |} else {
         |  $values[$i] = ${eval.value};
         |}
       """.stripMargin
    }

    val valuesCode = if (ctx.INPUT_ROW != null && ctx.currentVars == null) {
      ctx.splitExpressions(
        expressions = valCodes,
        funcName = "avroConverterCreateStruct",
        arguments = ("Object[]", values) :: ("InternalRow", ctx.INPUT_ROW) :: Nil)
    } else {
      valCodes.mkString("\n")
    }

    val tmpRow = ctx.freshName("tmpRowAvroConv")
    val newEncoderName = ctx.freshName("newEncoder")
    ev.copy(code =
      s"""
         |Object[] $values = new Object[${children.size}];
         |$valuesCode
         |final InternalRow $tmpRow = new $rowClass($values);
         |$values = null;
         |$bufferName.reset();
         |$encoderClassName $newEncoderName = $encoderFactoryClassName.get().binaryEncoder($bufferName, $encoderName);
         |byte[] ${ev.value} = $avroConverterExpression.serializeInternalRow($tmpRow, $bufferName, $newEncoderName);
       """.stripMargin, isNull = "false")
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
      case StringType => (item: Any) =>
        item match {
          case null => null
          case u: UTF8String => u.toString
          case _ => item // never here, I hope
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
        (item: Any) => {
          if (item == null) {
            null
          } else {
            val extractElemTypeFromUnion = externalSchema.map(s => eventualSubSchemaFromUnionWithNull(s))
            val elementConverter = createConverterToAvro(elementType, structName, recordNamespace, None, extractElemTypeFromUnion.map(s => s.getElementType))
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
          SchemaConverters.convertStructToAvro(structType, builder, recordNamespace)
        )

        val fieldConverters = structType.fields.filter(f => {
          if (fieldsToWrite.isDefined) {
            fieldsToWrite.get.contains(f.name)
          } else {
            true
          }
        }).map(field =>
          createConverterToAvro(field.dataType, field.name, recordNamespace, None, Some(schema.getField(field.name).schema()))
        )
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
        throw new IllegalArgumentException(s"Avro sub-schema ${s.getName} has UnionSchema which is not a simple NullSchema + primitive schema.")
      }
      otherType.head
    } else {
      s
    }
  }
}

