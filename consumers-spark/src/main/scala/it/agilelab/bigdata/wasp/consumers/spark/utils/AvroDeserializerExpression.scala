package it.agilelab.bigdata.wasp.consumers.spark.utils

import java.nio.ByteBuffer

import com.typesafe.config.Config
import it.agilelab.bigdata.wasp.core.utils.AvroSchemaConverters
import it.agilelab.bigdata.wasp.core.utils.AvroSchemaConverters.IncompatibleSchemaException
import it.agilelab.darwin.manager.AvroSchemaManagerFactory
import org.apache.avro.Schema
import org.apache.avro.Schema.Type._
import org.apache.avro.file.SeekableByteArrayInput
import org.apache.avro.generic.GenericData.Fixed
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericRecord}
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.apache.avro.util.Utf8
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, GenericInternalRow, UnaryExpression}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.slf4j
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/**
  * An `Expression` which deserializes a binary field encoded in Avro and returns the corresponding
  * representation in Spark.
  *
  * @param child             the `Expression` containing the binary Avro to be deserialized
  * @param schemaAvroJson    the JSON representation of the Avro schema
  * @param darwinConfig      the configuration for the AvroSchemaManager
  * @param avoidReevaluation this filed forces the Expression to be non-deterministic. Setting this to true
  *                          measn that this expression is executed only once even though the Optimizer creates
  *                          several copies of it (eg. it happens usually with CollapseProject if the value
  *                          returned by the expression is used several times, as with a selection of fields
  *                          of the struct returned). If you set this flag to false, the expression may be evaluated
  *                          once for each occurence of ti you see in the physical plan.
  */
case class AvroDeserializerExpression(
    child: Expression,
    schemaAvroJson: String,
    darwinConfig: Option[Config],
    avoidReevaluation: Boolean = true) extends UnaryExpression
    with ExpectsInputTypes {

  override def inputTypes: Seq[DataType] = Seq(BinaryType)

  @transient private lazy val avroSchemaManager = darwinConfig.map(AvroSchemaManagerFactory.initialize)

  @transient private lazy val userSchema = new Schema.Parser().parse(schemaAvroJson)

  private lazy val datumReader = new GenericDatumReader[Object](userSchema)

  private lazy val rowConverter = createConverterToSQL(userSchema, dataType)

  val mapDatumReader: mutable.HashMap[Long, GenericDatumReader[Object]] = mutable.HashMap.empty

  override def dataType: DataType = AvroSchemaConverters.toSqlType(userSchema).dataType

  // Set as non-deterministic in order to avoid re-execution of the deserialization
  // See CollapseProject + ProjectExec
  override lazy val deterministic: Boolean = !avoidReevaluation

  def avroDatumReader(avroValue: SeekableByteArrayInput): GenericDatumReader[Object] = {
    for {
      m      <- avroSchemaManager
      id     <- m.extractId(avroValue).right.toOption
      schema <- m.getSchema(id)
    } yield {
      mapDatumReader.getOrElseUpdate(id, new GenericDatumReader[Object](schema, userSchema))
    }
  }.getOrElse(datumReader)

  // We assume it returns an InternalRow since the input is a GenericRecord...
  def convertRecordToInternalRow(record: AnyRef): AnyRef = {
    rowConverter(record)
  }

  override protected def nullSafeEval(input: Any): Any = {
    val avroValue  = new SeekableByteArrayInput(input.asInstanceOf[Array[Byte]])
    val avroReader = avroDatumReader(avroValue)

    val decoder = DecoderFactory.get.binaryDecoder(avroValue, null)
    Try(avroReader.read(null, decoder)) match {
      case Success(value) => convertRecordToInternalRow(value)
      case Failure(exception) =>
        AvroDeserializerExpression.logger.warn(exception.getMessage)
        null
    }

  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {

    val decoderClassName        = classOf[BinaryDecoder].getName
    val decoderFactoryClassName = classOf[DecoderFactory].getName
    val decoderName             = ctx.addMutableState(decoderClassName, "decoder", varName => s"""$varName = null;""")

    val genericRecordClassName = classOf[Object].getName
    val genericRecordName =
      ctx.addMutableState(genericRecordClassName, "genRecord", varName => s"""$varName = null;""")

    val avroDecoderExpression =
      ctx.addReferenceObj("avroDecoderExpr", this, this.getClass.getName)

    val resultVarName = ctx.freshName("result")

    val genericReaderClassName = classOf[GenericDatumReader[GenericRecord]].getName
    val genericReaderName      = ctx.freshName("genericReader")

    val seekableClassName = classOf[SeekableByteArrayInput].getName
    val seekableInput     = ctx.freshName("seekableInput")

    val returnType = CodeGenerator.javaType(dataType)
    val boxedType  = CodeGenerator.boxedType(dataType)

    val childEval = child.genCode(ctx)

    val defaultValue = CodeGenerator.defaultValue(dataType, typedNull = true)

    val logger = classOf[AvroDeserializerExpression].getName + ".logger"

    ev.copy(
      code = code"""
              |${childEval.code}
              |$returnType ${ev.value} = $defaultValue;
              |if (!${childEval.isNull}) {
              |  final $seekableClassName $seekableInput = new $seekableClassName(${childEval.value});
              |  final $genericReaderClassName $genericReaderName = $avroDecoderExpression.avroDatumReader($seekableInput);
              |  $decoderName = $decoderFactoryClassName.get().binaryDecoder($seekableInput, $decoderName);
              |  try {
              |    $genericRecordName = ($genericRecordClassName) $genericReaderName.read($genericRecordName, $decoderName);
              |  } catch (java.io.IOException e) {
              |    $logger.info("Unable to deserialize Avro.", e);
              |    $genericRecordName = null
              |  }
              |  if ($genericRecordName){
              |    Object $resultVarName =  $avroDecoderExpression.convertRecordToInternalRow($genericRecordName);
              |    ${ev.value} = ($boxedType) $resultVarName;
              |  } else {
              |    ${ev.value} = $genericRecordName
              |  }
              |}
       """.stripMargin,
      isNull = childEval.isNull
    )
  }

  @inline
  val convertString: Any => AnyRef = { item =>
    if (item == null) null
    else {
      item match {
        case symbol: GenericData.EnumSymbol =>
          UTF8String.fromString(symbol.toString)
        case _ =>
          val avroUtf8 = item.asInstanceOf[Utf8]
          val byteBuffer = if (avroUtf8.getByteLength != avroUtf8.getBytes.length) {
            avroUtf8.getBytes.take(avroUtf8.getByteLength)
          } else {
            avroUtf8.getBytes
          }
          UTF8String.fromBytes(byteBuffer)
      }
    }
  }

  /**
    * Returns a converter function to convert row in avro format to GenericRow of catalyst.
    *
    * @param sourceAvroSchema Source schema before conversion inferred from avro file by passed in
    *                         by user.
    * @param targetSqlType    Target catalyst sql type after the conversion.
    * @return returns a converter function to convert row in avro format to GenericRow of catalyst.
    */
  private def createConverterToSQL(sourceAvroSchema: Schema, targetSqlType: DataType): AnyRef => AnyRef = {

    def createConverter(avroSchema: Schema, sqlType: DataType, path: List[String]): AnyRef => AnyRef = {
      val avroType = avroSchema.getType
      (sqlType, avroType) match {
        case (StringType, STRING) | (StringType, ENUM) =>
          (item: AnyRef) =>
            convertString(item)
          // Byte arrays are reused by avro, so we have to make a copy of them.
        case (IntegerType, INT) | (BooleanType, BOOLEAN) | (DoubleType, DOUBLE) | (FloatType, FLOAT) |
            (LongType, LONG) =>
          identity
        case (BinaryType, FIXED) =>
          (item: AnyRef) =>
            if (item == null) {
              null
            } else {
              item.asInstanceOf[Fixed].bytes().clone()
            }
        case (BinaryType, BYTES) =>
          (item: AnyRef) =>
            if (item == null) {
              null
            } else {
              val byteBuffer = item.asInstanceOf[ByteBuffer]
              val bytes      = new Array[Byte](byteBuffer.remaining)
              byteBuffer.get(bytes)
              bytes
            }

        case (struct: StructType, RECORD) =>
          val length           = struct.fields.length
          val converters       = new Array[AnyRef => AnyRef](length)
          val avroFieldIndexes = new Array[Int](length)
          var i                = 0
          while (i < length) {
            val sqlField  = struct.fields(i)
            val avroField = avroSchema.getField(sqlField.name)
            if (avroField != null) {
              val converter = createConverter(avroField.schema(), sqlField.dataType, path :+ sqlField.name)
              converters(i) = converter
              avroFieldIndexes(i) = avroField.pos()
            } else if (!sqlField.nullable) {
              throw new IncompatibleSchemaException(
                s"Cannot find non-nullable field ${sqlField.name} at path ${path.mkString(".")} " +
                  "in Avro schema\n" +
                  s"Source Avro schema: $sourceAvroSchema.\n" +
                  s"Target Catalyst type: $targetSqlType"
              )
            }
            i += 1
          }

          (item: AnyRef) => {
            if (item == null) {
              null
            } else {
              val record = item.asInstanceOf[GenericRecord]

              val result = new Array[Any](length)
              var i      = 0
              while (i < converters.length) {
                if (converters(i) != null) {
                  val converter = converters(i)
                  result(i) = converter(record.get(avroFieldIndexes(i)))
                }
                i += 1
              }
              new GenericInternalRow(result)
            }
          }
        case (arrayType: ArrayType, ARRAY) =>
          val elementConverter = createConverter(avroSchema.getElementType, arrayType.elementType, path)
          val allowsNull       = arrayType.containsNull
          // Specialize for simple types in order to use the unsafe representation and avoid copies
          arrayType.elementType match {
            case BooleanType =>
              (item: AnyRef) => {
                if (item == null) {
                  null
                } else {
                  val avroArray = item.asInstanceOf[GenericData.Array[Boolean]]
                  ArrayData.toArrayData(avroArray.toArray)
                }
              }
            case IntegerType =>
              (item: AnyRef) => {
                if (item == null) {
                  null
                } else {
                  val avroArray = item.asInstanceOf[GenericData.Array[Int]]
                  ArrayData.toArrayData(avroArray.toArray)
                }
              }
            case DoubleType =>
              (item: AnyRef) => {
                if (item == null) {
                  null
                } else {
                  val avroArray = item.asInstanceOf[GenericData.Array[Double]]
                  ArrayData.toArrayData(avroArray.toArray)
                }
              }
            case LongType =>
              (item: AnyRef) => {
                if (item == null) {
                  null
                } else {
                  val avroArray = item.asInstanceOf[GenericData.Array[Long]]
                  ArrayData.toArrayData(avroArray.toArray)
                }
              }
            case ByteType =>
              (item: AnyRef) => {
                if (item == null) {
                  null
                } else {
                  val avroArray = item.asInstanceOf[GenericData.Array[Byte]]
                  ArrayData.toArrayData(avroArray.toArray)
                }
              }
            case _ =>
              (item: AnyRef) => {
                if (item == null) {
                  null
                } else {
                  val convertedItems = item.asInstanceOf[java.lang.Iterable[AnyRef]].asScala.map { element =>
                    if (element == null && !allowsNull) {
                      throw new RuntimeException(
                        s"Array value at path ${path.mkString(".")} is not " +
                          "allowed to be null"
                      )
                    } else {
                      elementConverter(element)
                    }
                  }
                  ArrayData.toArrayData(convertedItems)
                }
              }
          }

          (item: AnyRef) => {
            if (item == null) {
              null
            } else {
              val convertedItems = item.asInstanceOf[java.lang.Iterable[AnyRef]].asScala.map { element =>
                if (element == null && !allowsNull) {
                  throw new RuntimeException(
                    s"Array value at path ${path.mkString(".")} is not " +
                      "allowed to be null"
                  )
                } else {
                  elementConverter(element)
                }
              }
              ArrayData.toArrayData(convertedItems)
            }
          }
        case (mapType: MapType, MAP) if mapType.keyType == StringType =>
          val valueConverter = createConverter(avroSchema.getValueType, mapType.valueType, path)
          (item: AnyRef) => {
            if (item == null) {
              null
            } else {
              ArrayBasedMapData(
                item.asInstanceOf[java.util.Map[AnyRef, AnyRef]],
                convertString,
                (x: Any) => valueConverter(x.asInstanceOf[AnyRef])
              )
            }
          }
        case (sparkSqlType, UNION) =>
          if (avroSchema.getTypes.asScala.exists(_.getType == NULL)) {
            val remainingUnionTypes = avroSchema.getTypes.asScala.filterNot(_.getType == NULL)
            if (remainingUnionTypes.size == 1) {
              createConverter(remainingUnionTypes.head, sparkSqlType, path)
            } else {
              createConverter(Schema.createUnion(remainingUnionTypes.asJava), sparkSqlType, path)
            }
          } else
            avroSchema.getTypes.asScala.map(_.getType) match {
              case Seq(_) => createConverter(avroSchema.getTypes.get(0), sparkSqlType, path)
              case Seq(a, b) if Set(a, b) == Set(INT, LONG) && sparkSqlType == LongType =>
                (item: AnyRef) => {
                  item match {
                    case null                 => null
                    case l: java.lang.Long    => l
                    case i: java.lang.Integer => java.lang.Long.valueOf(i.longValue())
                  }
                }
              case Seq(a, b) if Set(a, b) == Set(FLOAT, DOUBLE) && sparkSqlType == DoubleType =>
                (item: AnyRef) => {
                  item match {
                    case null                => null
                    case d: java.lang.Double => d
                    case f: java.lang.Float  => java.lang.Double.valueOf(f.doubleValue())
                  }
                }
              case other =>
                sparkSqlType match {
                  case t: StructType if t.fields.length == avroSchema.getTypes.size =>
                    val fieldConverters = t.fields.zip(avroSchema.getTypes.asScala).map {
                      case (field, schema) =>
                        createConverter(schema, field.dataType, path :+ field.name)
                    }

                    (item: AnyRef) =>
                      if (item == null) {
                        null
                      } else {
                        val i         = GenericData.get().resolveUnion(avroSchema, item)
                        val converted = new Array[Any](fieldConverters.length)
                        converted(i) = fieldConverters(i)(item)
                        new GenericInternalRow(converted)
                      }
                  case _ =>
                    throw new IncompatibleSchemaException(
                      s"Cannot convert Avro schema to catalyst type because schema at path " +
                        s"${path.mkString(".")} is not compatible " +
                        s"(avroType = $other, sqlType = $sparkSqlType). \n" +
                        s"Source Avro schema: $sourceAvroSchema.\n" +
                        s"Target Catalyst type: $targetSqlType"
                    )
                }
            }
        case (left, right) =>
          throw new IncompatibleSchemaException(
            s"Cannot convert Avro schema to catalyst type because schema at path " +
              s"${path.mkString(".")} is not compatible (avroType = $left, sqlType = $right). \n" +
              s"Source Avro schema: $sourceAvroSchema.\n" +
              s"Target Catalyst type: $targetSqlType"
          )
      }
    }

    createConverter(sourceAvroSchema, targetSqlType, List.empty[String])
  }

}

object AvroDeserializerExpression {
  val logger: slf4j.Logger = LoggerFactory.getLogger(AvroDeserializerExpression.getClass.getName)
}
