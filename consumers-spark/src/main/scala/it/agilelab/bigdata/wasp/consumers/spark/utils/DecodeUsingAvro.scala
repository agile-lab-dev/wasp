package it.agilelab.bigdata.wasp.consumers.spark.utils

import java.io.ByteArrayInputStream

import it.agilelab.darwin.manager.AvroSchemaManager
import it.agilelab.darwin.manager.util.AvroSingleObjectEncodingUtils
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericRecord}
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.apache.spark.sql.catalyst.expressions.{Expression, NonSQLExpression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{DataType, ObjectType}

import scala.reflect.ClassTag


case class DecodeUsingAvro[A](
                               child: Expression,
                               tag: ClassTag[A],
                               schema: String,
                               avroSchemaManager: () => AvroSchemaManager,
                               fromGenericRecord: GenericRecord => A
                             ) extends UnaryExpression
  with NonSQLExpression {

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // Code to initialize the serializer.
    val serializer = ctx.freshName("serializer")
    val serializerClass = classOf[DecodeUsingAvro.AvroDeserializer[A]].getName
    // If a I add a reference to the schema instance I get a TaskNotSerializableException
    // therefore I opted for passing by the string representation, it should be done only
    // once since it is in the mutable variable initialization of the code-gen.
    // I saw that the same has been done in the others AvroSerializerExpressions
    val schemaStringRef = ctx.addReferenceObj("schemaStr", schema, "java.lang.String")
    val schemaRef = ctx.freshName("schema")
    val schemaClass = classOf[Schema].getName
    val fromGenericRecordRef =
      ctx.addReferenceObj("fromGenericRecord", fromGenericRecord, classOf[GenericRecord => A].getName)
    val schemaManagerClass = classOf[AvroSchemaManager].getName
    val schemaManagerRef =
      ctx.addReferenceObj("schemaManager", avroSchemaManager, classOf[() => AvroSchemaManager].getName)
    val serializerInit =
      s"""
           $schemaClass $schemaRef = new $schemaClass.Parser().parse($schemaStringRef);
           $serializer = new $serializerClass($schemaRef, ($schemaManagerClass) $schemaManagerRef.apply(), $fromGenericRecordRef);
        """

    ctx.addMutableState(serializerClass, serializer, serializerInit)

    // Code to deserialize. this code is copy pasted from kryo expression encoder
    val input = child.genCode(ctx)
    val javaType = ctx.javaType(dataType)
    val deserialize =
      s"($javaType) $serializer.toObj(${input.value})"

    val code =
      s"""
      ${input.code}
      final $javaType ${ev.value} = ${input.isNull} ? ${ctx.defaultValue(javaType)} : $deserialize;
     """
    ev.copy(code = code, isNull = input.isNull)
  }

  override def dataType: DataType = ObjectType(tag.runtimeClass)

}

object DecodeUsingAvro {

  /**
    * Stateful avro deserializer: NOT thread safe
    */
  private final class AvroDeserializer[A](schema: Schema, avroSchemaManager: AvroSchemaManager, fromRecord: GenericRecord => A) {
    private[this] val genericRecord = new GenericData.Record(schema)
    private[this] var decoder: BinaryDecoder = _ // scalastyle:ignore

    def toObj(array: Array[Byte]): A = {
      val inputStream = new ByteArrayInputStream(array)
      val writerSchema = avroSchemaManager.getSchema(AvroSingleObjectEncodingUtils.extractId(inputStream).right.get).get
      val reader = new GenericDatumReader[GenericRecord](writerSchema, schema)
      decoder = DecoderFactory.get().binaryDecoder(inputStream, decoder)
      fromRecord(reader.read(genericRecord, decoder))
    }
  }

}
