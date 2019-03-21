package it.agilelab.bigdata.wasp.consumers.spark.utils


import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.util

import com.typesafe.config.Config
import it.agilelab.bigdata.wasp.consumers.spark.utils.RowToAvro.checkSchemas
import it.agilelab.darwin.manager.util.AvroSingleObjectEncodingUtils
import org.apache.avro.Schema.Type
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow, TimeZoneAwareExpression, UnsafeArrayData, UnsafeMapData}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.util.DateTimeUtils.SQLDate
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import it.agilelab.darwin.manager.{AvroSchemaManager, AvroSchemaManagerFactory}

import scala.collection.JavaConverters._


case class AvroConverterExpression(
    children: Seq[Expression],
    schemaAvroJson: Option[String] = None,
    avroSchemaManagerConfig: Option[Config] = None,
    useAvroSchemaManager: Boolean,
    inputSchema: StructType,
    structName: String,
    namespace: String,
    fieldsToWrite: Option[Set[String]],
    timeZoneId: Option[String] = None) extends Expression with TimeZoneAwareExpression {

  require(!useAvroSchemaManager || useAvroSchemaManager && avroSchemaManagerConfig.isDefined,
    "if useAvroSchemaManager is true avroSchemaManagerConfig must have a value")

  schemaAvroJson.foreach(s => checkSchemas(inputSchema, s))

  @transient private lazy val externalSchema: Option[Schema] = schemaAvroJson.map(s => new Schema.Parser().parse(s))


  @transient private lazy val converter =
    createConverterToAvro(inputSchema, structName, namespace, fieldsToWrite, externalSchema)
  @transient private lazy val actualSchema: Schema = getSchema
  @transient private lazy val schemaId = {
    AvroSchemaManagerFactory.getInstance(avroSchemaManagerConfig.get).getId(actualSchema)
  }

  private def getSchema: Schema = {
    val builder = SchemaBuilder.record(structName).namespace(namespace)
    schemaAvroJson.map(s => new Schema.Parser().parse(s)).getOrElse(
      SchemaConverters.convertStructToAvro(inputSchema, builder, namespace)
    )
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children == Nil) {
      TypeCheckResult.TypeCheckFailure("input to function AvroConverterExpression cannot be empty")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression = copy(timeZoneId = Some(timeZoneId))

  override def nullable: Boolean = true

  def serializeInternalRow(row: InternalRow): Array[Byte] = {
    val output = new ByteArrayOutputStream()
    val value: GenericRecord = converter(row).asInstanceOf[GenericRecord]

    val encoder = EncoderFactory.get.binaryEncoder(output, null)
    val writer = new GenericDatumWriter[GenericRecord](actualSchema)
    writer.write(value, encoder)

    encoder.flush()

    if (useAvroSchemaManager) {
      AvroSingleObjectEncodingUtils.generateAvroSingleObjectEncoded(output.toByteArray, schemaId)
    } else {
      output.toByteArray
    }
  }

  override def eval(input: InternalRow): Any = {
    serializeInternalRow(InternalRow.fromSeq(children.map(_.eval(input))))
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val rowClass = classOf[GenericInternalRow].getName
    val values = ctx.freshName("values")
    val avroConverterExpression =
      ctx.addReferenceObj("avroConverterExpr", this, this.getClass.getName)
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
    val valuesCode = ctx.splitExpressions(
      expressions = valCodes,
      funcName = "avroConverterCreateStruct",
      arguments = ("Object[]", values) :: ("InternalRow", ctx.INPUT_ROW) :: Nil)

    val tmpRow = ctx.freshName("tmpRowAvroConv")
    ev.copy(code =
      s"""
         |Object[] $values = new Object[${children.size}];
         |$valuesCode
         |final InternalRow $tmpRow = new $rowClass($values);
         |$values = null;
         |byte[] ${ev.value} = $avroConverterExpression.serializeInternalRow($tmpRow);
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
      case TimestampType => (item:Any) =>
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
            val sourceArray = item.asInstanceOf[UnsafeArrayData].array()
            val sourceArraySize = sourceArray.size
            val targetArray = new util.ArrayList[Any](sourceArraySize)
            var idx = 0
            while (idx < sourceArraySize) {
              targetArray.add(idx, elementConverter(sourceArray(idx)))
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
            val keys = item.asInstanceOf[UnsafeMapData].keyArray()
            val values = item.asInstanceOf[UnsafeMapData].valueArray()
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

