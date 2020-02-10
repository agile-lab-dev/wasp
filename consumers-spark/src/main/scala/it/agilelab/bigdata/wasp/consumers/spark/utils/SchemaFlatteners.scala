package it.agilelab.bigdata.wasp.consumers.spark.utils

import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

object SchemaFlatteners {

  object Avro {
    def flattenSchema(schema: Schema, prefix: String): List[(String, String)] = {
      schema.getFields.asScala.toList.flatMap(handleField(_, prefix, insideMap = false))
    }

    @scala.annotation.tailrec
    private final def handleField(field: Schema.Field,
                                  prefix: String,
                                  insideMap: Boolean): Seq[(String, String)] = {

      val name = prefix + field.name
      val schemaType = field.schema().getType
      schemaType match {
        case Type.RECORD =>
          // complex type: recurse
          val newPrefix = name + "."
          flattenSchema(field.schema, newPrefix)
        case Type.UNION =>
          // union type: check that is simple enough, recurse if necessary
          // drop NullSchema, fail if more than one Schema remains afterwards
          val nonNullSchemas = field.schema().getTypes.asScala.filter(_.getType != Type.NULL)
          if (nonNullSchemas.length > 1) {
            throw new IllegalArgumentException(
              s"Field $name in the Avro schema has UnionSchema ${field.schema()} " +
                "which is not a simple NullSchema + primitive schema.")
          }
          val remainingSchema = nonNullSchemas.head
          // recurse if necessary
          if (remainingSchema.getType == Type.RECORD) {
            // nullable record, recurse
            val newPrefix = name + "."
            flattenSchema(remainingSchema, newPrefix)
          } else {
            // simple type: create tuple
            val tpe = typeToString(remainingSchema.getType)
            Seq((name, mapTpe(tpe, insideMap))) // sequence with a single element because we're in a flatMap
          }
        case Type.MAP =>
          field.schema.getValueType.getType match {
            case Type.RECORD => flattenSchema(field.schema().getValueType, name + ".map_")
            case _ =>
              handleField(new Schema.Field(field.name(), field.schema().getValueType, "", null: Object), prefix, insideMap = true)
          }
        case _ =>
          // simple type: create tuple
          val tpe = typeToString(schemaType)
          Seq((name, mapTpe(tpe, insideMap))) // sequence with a single element because we're in a flatMap
      }
    }

    def typeToString(tpe: Type): String = tpe match {
      case Type.BOOLEAN => booleanType
      case Type.BYTES => binaryType
      case Type.INT => intType
      case Type.LONG => longType
      case Type.FLOAT => floatType
      case Type.DOUBLE => doubleType
      case Type.STRING => stringType
      case Type.ARRAY => arrayType
      case _ => throw new IllegalArgumentException(s"$tpe is not a supported primitive type for Avro")
    }
  }

  object Spark {
    def flattenSchema(schema: StructType, prefix: String): Seq[(String, String)] = {
      schema.fields.toList.flatMap(handleField(_, prefix, insideMap = false))
    }

    @scala.annotation.tailrec
    def handleField(field: StructField,
                    prefix: String,
                    insideMap: Boolean): Seq[(String, String)] = {
      val name = prefix + field.name
      field.dataType match {
        case struct: StructType =>
          flattenSchema(struct, name + ".")
        case MapType(StringType, valueType: StructType, _) => flattenSchema(valueType, name + ".map_")
        case MapType(StringType, valueType, _) =>
          handleField(StructField(field.name, valueType), prefix, insideMap = true)
        case mt: MapType =>
          throw new IllegalArgumentException(s"Maps with non-String keys are not supported by avro, cannot handle $mt")
        case other =>
          // simple type: create tuple
          val tpe = typeToString(other)
          Seq((name, mapTpe(tpe, insideMap))) // sequence with a single element because we're in a flatMap
      }
    }

    def typeToString(dataType: DataType): String = dataType match {
      case _: BooleanType => booleanType
      case _: ByteType => byteType
      case _: IntegerType => intType
      case _: LongType => longType
      case _: FloatType => floatType
      case _: DoubleType => doubleType
      case _: StringType => stringType
      case _: ArrayType => arrayType
      case _: BinaryType => binaryType
      case _: DateType => longType
      case _: TimestampType => longType
      case _ => throw new IllegalArgumentException(s"$dataType is not a supported primitive type for Catalyst")
    }
  }

  private def mapTpe(s: String, b: Boolean) = if (b) "map_" + s else s

  private val booleanType = "boolean"
  private val byteType = "byte"
  private val intType = "int"
  private val longType = "long"
  private val floatType = "float"
  private val doubleType = "double"
  private val stringType = "string"
  private val arrayType = "array"
  private val binaryType = "bytes"


}
