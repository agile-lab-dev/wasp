package it.agilelab.bigdata.wasp.core.utils
/*
 * Copyright 2014 Databricks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.avro.Schema.Type._
import org.apache.avro.SchemaBuilder._
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

/**
  * This object contains method that are used to convert sparkSQL schemas to avro schemas and vice
  * versa.
  */
object AvroSchemaConverters extends AvroSchemaConverters
trait AvroSchemaConverters {

  class IncompatibleSchemaException(msg: String, ex: Throwable = null) extends Exception(msg, ex)

  case class SchemaType(dataType: DataType, nullable: Boolean)

  /**
    * This function takes an avro schema and returns a sql schema.
    */
  def toSqlType(avroSchema: Schema): SchemaType = {
    avroSchema.getType match {
      case INT     => SchemaType(IntegerType, nullable = false)
      case STRING  => SchemaType(StringType, nullable = false)
      case BOOLEAN => SchemaType(BooleanType, nullable = false)
      case BYTES   => SchemaType(BinaryType, nullable = false)
      case DOUBLE  => SchemaType(DoubleType, nullable = false)
      case FLOAT   => SchemaType(FloatType, nullable = false)
      case LONG    => SchemaType(LongType, nullable = false)
      case FIXED   => SchemaType(BinaryType, nullable = false)
      case ENUM    => SchemaType(StringType, nullable = false)

      case RECORD =>
        val fields = avroSchema.getFields.asScala.map { f =>
          val schemaType = toSqlType(f.schema())
          StructField(f.name, schemaType.dataType, schemaType.nullable)
        }

        SchemaType(StructType(fields), nullable = false)

      case ARRAY =>
        val schemaType = toSqlType(avroSchema.getElementType)
        SchemaType(ArrayType(schemaType.dataType, containsNull = schemaType.nullable), nullable = false)

      case MAP =>
        val schemaType = toSqlType(avroSchema.getValueType)
        SchemaType(MapType(StringType, schemaType.dataType, valueContainsNull = schemaType.nullable), nullable = false)

      case UNION =>
        if (avroSchema.getTypes.asScala.exists(_.getType == NULL)) {
          // In case of a union with null, eliminate it and make a recursive call
          val remainingUnionTypes = avroSchema.getTypes.asScala.filterNot(_.getType == NULL)
          if (remainingUnionTypes.size == 1) {
            toSqlType(remainingUnionTypes.head).copy(nullable = true)
          } else {
            toSqlType(Schema.createUnion(remainingUnionTypes.asJava)).copy(nullable = true)
          }
        } else
          avroSchema.getTypes.asScala.map(_.getType) match {
            case Seq(t1) =>
              toSqlType(avroSchema.getTypes.get(0))
            case Seq(t1, t2) if Set(t1, t2) == Set(INT, LONG) =>
              SchemaType(LongType, nullable = false)
            case Seq(t1, t2) if Set(t1, t2) == Set(FLOAT, DOUBLE) =>
              SchemaType(DoubleType, nullable = false)
            case _ =>
              // Convert complex unions to struct types where field names are member0, member1, etc.
              // This is consistent with the behavior when converting between Avro and Parquet.
              val fields = avroSchema.getTypes.asScala.zipWithIndex.map {
                case (s, i) =>
                  val schemaType = toSqlType(s)
                  // All fields are nullable because only one of them is set at a time
                  StructField(s"member$i", schemaType.dataType, nullable = true)
              }

              SchemaType(StructType(fields), nullable = false)
          }

      case other => throw new IncompatibleSchemaException(s"Unsupported type $other")
    }
  }

  /**
    * This function converts sparkSQL StructType into avro schema. This method uses two other
    * converter methods in order to do the conversion.
    */
  def convertStructToAvro[T](structType: StructType, schemaBuilder: RecordBuilder[T], recordNamespace: String): T = {
    val fieldsAssembler: FieldAssembler[T] = schemaBuilder.fields()
    structType.fields.foreach { field =>
      val newField = fieldsAssembler.name(field.name).`type`()

      if (field.nullable) {
        convertFieldTypeToAvro(field.dataType, newField.nullable(), field.name, recordNamespace).noDefault
      } else {
        convertFieldTypeToAvro(field.dataType, newField, field.name, recordNamespace).noDefault
      }
    }
    fieldsAssembler.endRecord()
  }

  /**
    * This function is used to convert some sparkSQL type to avro type. Note that this function won't
    * be used to construct fields of avro record (convertFieldTypeToAvro is used for that).
    */
  def convertTypeToAvro[T](
      dataType: DataType,
      schemaBuilder: BaseTypeBuilder[T],
      structName: String,
      recordNamespace: String
  ): T = {
    dataType match {
      case ByteType       => schemaBuilder.intType()
      case ShortType      => schemaBuilder.intType()
      case IntegerType    => schemaBuilder.intType()
      case LongType       => schemaBuilder.longType()
      case FloatType      => schemaBuilder.floatType()
      case DoubleType     => schemaBuilder.doubleType()
      case _: DecimalType => schemaBuilder.stringType()
      case StringType     => schemaBuilder.stringType()
      case BinaryType     => schemaBuilder.bytesType()
      case BooleanType    => schemaBuilder.booleanType()
      case TimestampType  => schemaBuilder.longType()
      case DateType       => schemaBuilder.longType()

      case ArrayType(elementType, _) =>
        val builder       = getSchemaBuilder(dataType.asInstanceOf[ArrayType].containsNull)
        val elementSchema = convertTypeToAvro(elementType, builder, structName, recordNamespace)
        schemaBuilder.array().items(elementSchema)

      case MapType(StringType, valueType, _) =>
        val builder     = getSchemaBuilder(dataType.asInstanceOf[MapType].valueContainsNull)
        val valueSchema = convertTypeToAvro(valueType, builder, structName, recordNamespace)
        schemaBuilder.map().values(valueSchema)

      case structType: StructType =>
        convertStructToAvro(structType, schemaBuilder.record(structName).namespace(recordNamespace), recordNamespace)

      case other => throw new IncompatibleSchemaException(s"Unexpected type $dataType.")
    }
  }

  /**
    * This function is used to construct fields of the avro record, where schema of the field is
    * specified by avro representation of dataType. Since builders for record fields are different
    * from those for everything else, we have to use a separate method.
    */
  private def convertFieldTypeToAvro[T](
      dataType: DataType,
      newFieldBuilder: BaseFieldTypeBuilder[T],
      structName: String,
      recordNamespace: String
  ): FieldDefault[T, _] = {
    dataType match {
      case ByteType       => newFieldBuilder.intType()
      case ShortType      => newFieldBuilder.intType()
      case IntegerType    => newFieldBuilder.intType()
      case LongType       => newFieldBuilder.longType()
      case FloatType      => newFieldBuilder.floatType()
      case DoubleType     => newFieldBuilder.doubleType()
      case _: DecimalType => newFieldBuilder.stringType()
      case StringType     => newFieldBuilder.stringType()
      case BinaryType     => newFieldBuilder.bytesType()
      case BooleanType    => newFieldBuilder.booleanType()
      case TimestampType  => newFieldBuilder.longType()
      case DateType       => newFieldBuilder.longType()

      case ArrayType(elementType, _) =>
        val builder       = getSchemaBuilder(dataType.asInstanceOf[ArrayType].containsNull)
        val elementSchema = convertTypeToAvro(elementType, builder, structName, recordNamespace)
        newFieldBuilder.array().items(elementSchema)

      case MapType(StringType, valueType, _) =>
        val builder     = getSchemaBuilder(dataType.asInstanceOf[MapType].valueContainsNull)
        val valueSchema = convertTypeToAvro(valueType, builder, structName, recordNamespace)
        newFieldBuilder.map().values(valueSchema)

      case structType: StructType =>
        convertStructToAvro(structType, newFieldBuilder.record(structName).namespace(recordNamespace), recordNamespace)

      case other => throw new IncompatibleSchemaException(s"Unexpected type $dataType.")
    }
  }

  private def getSchemaBuilder(isNullable: Boolean): BaseTypeBuilder[Schema] = {
    if (isNullable) {
      SchemaBuilder.builder().nullable()
    } else {
      SchemaBuilder.builder()
    }
  }
}
