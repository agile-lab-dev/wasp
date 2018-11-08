package it.agilelab.bigdata.wasp.consumers.spark.utils

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.sql.{Date, Timestamp}
import java.util

import com.typesafe.config.Config
import it.agilelab.darwin.manager.AvroSchemaManager
import org.apache.avro.Schema.Type
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._
import scala.collection.immutable.Map

/**
  * Spark Row to serialized Avro converter class.
  * The two schemas must be compatible, otherwise an IllegalArgumentExceptio will be thrown at object creation time.
  * The schemas are compatible if:
  *   - any field appearing int the Avro schema exists in the Spark schema
  *   - any such field has the same type in both schemas
  *   - every field is of type boolean, int, long, float, double, string, complex (StructType for Spark, Record for Avro)
  *
  * @author Mattia Bertorello
  * @author Nicolò Bidotti
  */
case class RowToAvro(schema: StructType,
                     structName: String,
                     useAvroSchemaManager: Boolean,
                     recordNamespace: String,
                     fieldsToWrite: Option[Set[String]] = None,
                     schemaAvroJson: Option[String] = None,
                     avroSchemaManagerConfig: Option[Config] = None) {

  require(!useAvroSchemaManager || useAvroSchemaManager && avroSchemaManagerConfig.isDefined,
    "if useAvroSchemaManager is true avroSchemaManagerConfig must have a value")

  import RowToAvro._

  // check schemas are  compatible
  schemaAvroJson.foreach(s => checkSchemas(schema, s))

  // these fields are lazy because otherwise we would not be able to serialize instances of the case class
  private lazy val externalSchema: Option[Schema] = schemaAvroJson.map(s => new Schema.Parser().parse(s))
  private lazy val converter: (Any) => Any = createConverterToAvro(schema, structName, recordNamespace, fieldsToWrite, externalSchema)
  private lazy val actualSchema: Schema = getSchema()

  def getSchema(): Schema = {
    val builder = SchemaBuilder.record(structName).namespace(recordNamespace)
    schemaAvroJson.map(s => new Schema.Parser().parse(s)).getOrElse(
      SchemaConverters.convertStructToAvro(schema, builder, recordNamespace)
    )
  }

  /** Serialize Avro.
    * If use [[AvroSchemaManager]], first 2 byte is a MAGIC NUMBER (C3 01), 8 byte is a hash of [[Schema]] and other is avro serialized.
    *
    * @param row
    * @return avro serialize
    */
  def write(row: Row): Array[Byte] = {
    val output = new ByteArrayOutputStream()
    val value: GenericRecord = converter(row).asInstanceOf[GenericRecord]

    val encoder = EncoderFactory.get.binaryEncoder(output, null)
    val writer = new GenericDatumWriter[GenericRecord](actualSchema)
    writer.write(value, encoder)

    encoder.flush()
    output.toByteArray

    if (useAvroSchemaManager) {
      AvroSchemaManager.instance(avroSchemaManagerConfig.get)
      AvroSchemaManager.generateAvroSingleObjectEncoded(output.toByteArray, AvroSchemaManager.getId(actualSchema))
    }
    else
      output.toByteArray
  }


  /**
    * This function constructs converter function for a given sparkSQL datatype. This is used in
    * writing Avro records out to disk
    */
  private def createConverterToAvro(
                                       dataType: DataType,
                                       structName: String,
                                       recordNamespace: String,
                                       fieldsToWrite: Option[Set[String]],
                                       externalSchema: Option[Schema]): (Any) => Any = {
    dataType match {
      case BinaryType => (item: Any) =>
        item match {
          case null => null
          case bytes: Array[Byte] => ByteBuffer.wrap(bytes)
        }
      case ByteType | ShortType | IntegerType | LongType |
           FloatType | DoubleType | StringType | BooleanType => identity
      case _: DecimalType => (item: Any) => if (item == null) null else item.toString
      case TimestampType => (item: Any) =>
        if (item == null) null else item.asInstanceOf[Timestamp].getTime
      case DateType => (item: Any) =>
        if (item == null) null else item.asInstanceOf[Date].getTime
      case ArrayType(elementType, _) =>
        (item: Any) => {
          if (item == null) {
            null
          } else {
            val extractElemTypeFromUnion = externalSchema.map(s => eventualSubSchemaFromUnionWithNull(s))
            val elementConverter = createConverterToAvro(elementType, structName, recordNamespace, None, extractElemTypeFromUnion.map(s => s.getElementType))
            val sourceArray = item.asInstanceOf[Seq[Any]]
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
            item.asInstanceOf[Map[String, Any]].foreach { case (key, value) =>
              javaMap.put(key, valueConverter(value))
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
            val fieldNamesIterator = dataType.asInstanceOf[StructType].fieldNames.iterator
            val rowIterator = item.asInstanceOf[Row].toSeq.iterator

            while (convertersIterator.hasNext) {
              val converter = convertersIterator.next()
              record.put(fieldNamesIterator.next(), converter(rowIterator.next()))
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

object RowToAvro {
  private val booleanType = "boolean"
  private val byteType = "byte"
  private val intType = "int"
  private val longType = "long"
  private val floatType = "float"
  private val doubleType = "double"
  private val stringType = "string"
  private val arrayType = "array"
  private val binaryType = "bytes"

  /**
    * Ensures schemas are compatible, that is:
    *   - any field appearing int the Avro schema exists in the Spark schema
    *   - any such field has the same type in both schemas
    */
  def checkSchemas(schemaSpark: StructType,
                   schemaAvroJson: String): Unit = {

    // parse avro schema from json
    val schemaAvro: Schema = new Schema.Parser().parse(schemaAvroJson)

    // flatten schemas, convert spark field list into a map
    val sparkFields: Map[String, String] = flattenSparkSchema(schemaSpark).toMap
    // the key is the field name, the value is the field type
    val avroFields: List[(String, String)] = flattenAvroSchema(schemaAvro)

    // iterate over the Avro field list. If any is missing from the Spark schema, or has a different type, throw an exception.
    avroFields foreach {
      case (fieldName, fieldAvroType) => {
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

  /**
    * Flattens the provided Spark schema, returning a list of tuples.
    * The first element of each tuple is the field name, the second one is the field type.
    * Nested fields are represented using dot notation.
    */
  def flattenSparkSchema(schema: StructType, prefix: String = ""): List[(String, String)] = {
    schema.fields.toList.flatMap {
      field => {
        val name = prefix + field.name
        val dataType = field.dataType
        if (dataType.isInstanceOf[StructType]) {
          // complex type: recurse
          val prefix = name + "."
          flattenSparkSchema(dataType.asInstanceOf[StructType], prefix)
        } else {
          // simple type: create tuple
          val tpe = sparkTypeToString(dataType)
          Seq((name, tpe)) // sequence with a single element because we're in a flatMap
        }
      }
    }
  }

  def sparkTypeToString(dataType: DataType): String = dataType match {
    case _: BooleanType => booleanType
    case _: ByteType => byteType
    case _: IntegerType => intType
    case _: LongType => longType
    case _: FloatType => floatType
    case _: DoubleType => doubleType
    case _: StringType => stringType
    case _: ArrayType => arrayType
    case _: BinaryType => binaryType
  }

  /**
    * Flattens the provided Avro schema, returning a list of tuples.
    * The first element of each tuple is the field name, the second one is the field type.
    * Nested fields are represented using dot notation.
    */
  def flattenAvroSchema(schema: Schema, prefix: String = ""): List[(String, String)] = {
    schema.getFields.asScala.toList.flatMap {
      field => {
        val name = prefix + field.name
        val schemaType = field.schema().getType
        if (schemaType == Type.RECORD) {
          // complex type: recurse
          val prefix = name + "."
          flattenAvroSchema(field.schema, prefix)
        } else if (schemaType == Type.UNION) {
          // union type: check that is simple enough, recurse if necessary
          // drop NullSchema, fail if more than one Schema remains afterwards
          val nonNullSchemas = field.schema().getTypes.asScala.filter(_.getType != Type.NULL)
          if (nonNullSchemas.length > 1) {
            throw new IllegalArgumentException(s"Field $name in the Avro schema has UnionSchema ${field.schema()} which is not a simple NullSchema + primitive schema.")
          }
          val remainingSchema = nonNullSchemas.head
          // recurse if necessary
          if (remainingSchema.getType == Type.RECORD) {
            // nullable record, recurse
            val prefix = name + "."
            flattenAvroSchema(remainingSchema, prefix)
          } else {
            // simple type: create tuple
            val tpe = avroTypeToString(remainingSchema.getType)
            Seq((name, tpe)) // sequence with a single element because we're in a flatMap
          }
        } else {
          // simple type: create tuple
          val tpe = avroTypeToString(schemaType)
          Seq((name, tpe)) // sequence with a single element because we're in a flatMap
        }
      }
    }
  }

  def avroTypeToString(tpe: Type): String = tpe match {
    case Type.BOOLEAN => booleanType
    case Type.BYTES => binaryType
    case Type.INT => intType
    case Type.LONG => longType
    case Type.FLOAT => floatType
    case Type.DOUBLE => doubleType
    case Type.STRING => stringType
    case Type.ARRAY => arrayType
  }

}
