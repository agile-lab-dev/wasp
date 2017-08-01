package it.agilelab.bigdata.wasp.core.utils

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.sql.Timestamp
import java.util

import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.immutable.Map
import scala.collection.JavaConverters._

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
                     schemaAvroJson: String)  {
  import RowToAvro._
  
  // check schemas are  compatible
  checkSchemas(schema, schemaAvroJson)
  
  // these fields are lazy because otherwise we would not be able to serialize instances of the case class
  private lazy val schemaAvro: Schema = new Schema.Parser().parse(schemaAvroJson)
  private lazy val converter: (Any) => Any = createConverterToAvro(schema, schemaAvro)


  def write(row: Row): Array[Byte] = {
    val output = new ByteArrayOutputStream()
    val value: GenericRecord = converter(row).asInstanceOf[GenericRecord]
    var writer = new DataFileWriter(new GenericDatumWriter[GenericRecord]())
    writer.create(schemaAvro, output)
    writer.append(value)
    writer.flush()
    output.toByteArray

  }


  /**
    * This function constructs a converter function for a given sparkSQL datatype. This is used in
    * writing Avro records out to disk.
    *
    * Warning: while it does work with nested types, it will not work with nested types used as array elements!!
    */
  private def createConverterToAvro(dataType: DataType,
                                    schema: Schema,
                                    fieldName: String = ""): (Any) => Any = {
    dataType match {
      case BinaryType => (item: Any) => item match {
        case null => null
        case bytes: Array[Byte] => ByteBuffer.wrap(bytes)
      }
      case ByteType | ShortType | IntegerType | LongType |
           FloatType | DoubleType | StringType | BooleanType => identity
      case _: DecimalType => (item: Any) => if (item == null) null else item.toString
      case TimestampType => (item: Any) =>
        if (item == null) null else item.asInstanceOf[Timestamp].getTime
      case ArrayType(elementType, _) =>
        val elementConverter = createConverterToAvro(elementType, schema)
        (item: Any) => {
          if (item == null) {
            null
          } else {
            val sourceArray = item.asInstanceOf[Seq[Any]]
            val sourceArraySize = sourceArray.size
            val targetArray = new Array[Any](sourceArraySize)
            var idx = 0
            while (idx < sourceArraySize) {
              targetArray(idx) = elementConverter(sourceArray(idx))
              idx += 1
            }
            targetArray
          }
        }
      case MapType(StringType, valueType, _) =>
        val valueConverter = createConverterToAvro(valueType, schema)
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
        if (fieldName == "") { // top-level
          val fieldConverters = structType.fields.map(field =>
            createConverterToAvro(field.dataType, schema, field.name))
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
                val fieldName = fieldNamesIterator.next()
                if (schema.getField(fieldName) != null) {
                  record.put(fieldName, converter(rowIterator.next()))
          
                }
              }
              record
            }
          }
        } else { // nested struct
          // find the nested schema corresponding to the field name, handling union types
          val unknownNestedSchema = schema.getField(fieldName).schema()
          val nestedSchema = if (unknownNestedSchema.getType == Schema.Type.UNION) {
            unknownNestedSchema.getTypes.asScala.filter(_.getType != Schema.Type.NULL).head
          } else {
            unknownNestedSchema
          }
          // build field converters for nested fields
          val fieldConverters = structType.fields.map(field =>
            createConverterToAvro(field.dataType, nestedSchema, field.name))
          // return appropriate converter using nested schema
          (item: Any) => {
            if (item == null) {
              null
            } else {
              val record = new Record(nestedSchema)
              val convertersIterator = fieldConverters.iterator
              val fieldNamesIterator = dataType.asInstanceOf[StructType].fieldNames.iterator
              val rowIterator = item.asInstanceOf[Row].toSeq.iterator
      
              while (convertersIterator.hasNext) {
                val converter = convertersIterator.next()
                val fieldName = fieldNamesIterator.next()
                if (nestedSchema.getField(fieldName) != null) {
                  record.put(fieldName, converter(rowIterator.next()))
          
                }
              }
              record
            }
          }
        }
    }
  }
}
object RowToAvro {
  private val booleanType = "boolean"
  private val byteType    = "byte"
  private val intType     = "int"
  private val longType    = "long"
  private val floatType   = "float"
  private val doubleType  = "double"
  private val stringType  = "string"
	
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
    val sparkFields: Map[String, String] = flattenSparkSchema(schemaSpark).toMap // the key is the field name, the value is the field type
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
        if (dataType.isInstanceOf[StructType]) { // complex type: recurse
          val prefix = name + "."
          flattenSparkSchema(dataType.asInstanceOf[StructType], prefix)
        } else { // simple type: create tuple
          val tpe = sparkTypeToString(dataType)
          Seq((name, tpe)) // sequence with a single element because we're in a flatMap
        }
      }
    }
  }
  
  def sparkTypeToString(dataType: DataType): String = dataType match {
    case _ : BooleanType => booleanType
    case _ : ByteType    => byteType
    case _ : IntegerType => intType
    case _ : LongType    => longType
    case _ : FloatType   => floatType
    case _ : DoubleType  => doubleType
    case _ : StringType  => stringType
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
        if (schemaType == Type.RECORD) { // complex type: recurse
          val prefix = name + "."
          flattenAvroSchema(field.schema, prefix)
        } else if (schemaType == Type.UNION) { // union type: check that is simple enough, recurse if necessary
          // drop NullSchema, fail if more than one Schema remains afterwards
          val nonNullSchemas = field.schema().getTypes.asScala.filter(_.getType != Type.NULL)
          if (nonNullSchemas.length > 1) {
            throw new IllegalArgumentException(s"Field $name in the Avro schema has UnionSchema ${field.schema()} which is not a simple NullSchema + primitive schema.")
          }
          val remainingSchema = nonNullSchemas.head
          // recurse if necessary
          if (remainingSchema.getType == Type.RECORD) { // nullable record, recurse
            val prefix = name + "."
            flattenAvroSchema(remainingSchema, prefix)
          } else { // simple type: create tuple
            val tpe = avroTypeToString(remainingSchema.getType)
            Seq((name, tpe)) // sequence with a single element because we're in a flatMap
          }
        } else { // simple type: create tuple
          val tpe = avroTypeToString(schemaType)
          Seq((name, tpe)) // sequence with a single element because we're in a flatMap
        }
      }
    }
  }
  
  def avroTypeToString(tpe: Type): String = tpe match {
    case Type.BOOLEAN => booleanType
    case Type.BYTES   => byteType
    case Type.INT     => intType
    case Type.LONG    => longType
    case Type.FLOAT   => floatType
    case Type.DOUBLE  => doubleType
    case Type.STRING  => stringType
  }
}