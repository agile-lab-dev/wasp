package it.agilelab.bigdata.wasp.consumers.spark.utils

import java.io.ByteArrayInputStream

import it.agilelab.bigdata.wasp.consumers.spark.utils.DecodeUsingAvro.AvroDeserializer
import it.agilelab.darwin.manager.AvroSchemaManager
import it.agilelab.darwin.manager.util.AvroSingleObjectEncodingUtils
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericRecord}
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.apache.spark.sql.catalyst.expressions.{Expression, NonSQLExpression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.types.{DataType, ObjectType}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._

import scala.reflect.ClassTag

case class DecodeUsingAvro[A](
                               child: Expression,
                               tag: ClassTag[A],
                               schema: String,
                               avroSchemaManager: () => AvroSchemaManager,
                               fromGenericRecord: GenericRecord => A
                             ) extends UnaryExpression
  with NonSQLExpression {

  private lazy val deserializer = new AvroDeserializer[A](new Schema.Parser().parse(schema),
    avroSchemaManager(),
    fromGenericRecord)

  override protected def nullSafeEval(input: Any): Any = {
    deserializer.toObj(input.asInstanceOf[Array[Byte]])
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // Code to initialize the serializer.
    val serializerClass = classOf[DecodeUsingAvro.AvroDeserializer[A]].getName
    // If a I add a reference to the schema instance I get a TaskNotSerializableException
    // therefore I opted for passing by the string representation, it should be done only
    // once since it is in the mutable variable initialization of the code-gen.
    // I saw that the same has been done in the others AvroSerializerExpressions
    val schemaStringRef = ctx.addReferenceObj("schemaStr", schema, "java.lang.String")
    val schemaClass = classOf[Schema].getName
    val fromGenericRecordRef =
      ctx.addReferenceObj("fromGenericRecord", fromGenericRecord, classOf[GenericRecord => A].getName)
    val schemaManagerClass = classOf[AvroSchemaManager].getName
    val schemaManagerFactoryRef =
      ctx.addReferenceObj("schemaManagerFactory", avroSchemaManager, classOf[() => AvroSchemaManager].getName)

    val schemaRef = "schema"
    val schemaManagerRef = "schemaManager"

    ctx.addImmutableStateIfNotExists(schemaManagerClass, schemaManagerRef, v =>
      s"$v = ($schemaManagerClass) $schemaManagerFactoryRef.apply();"
    )
    ctx.addImmutableStateIfNotExists(schemaClass, schemaRef, v =>
      s"$v = new $schemaClass.Parser().parse($schemaStringRef);"
    )

    val serializer = ctx.addMutableState(serializerClass, "serializer", v =>
      s"$v = new $serializerClass($schemaRef, $schemaManagerRef, $fromGenericRecordRef);"
    )

    // Code to deserialize. this code is copy pasted from kryo expression encoder
    val input = child.genCode(ctx)
    val javaType = CodeGenerator.javaType(dataType)
    val deserialize =
      s"($javaType) $serializer.toObj(${input.value})"


    val code = input.code +
      code"""
      final $javaType ${ev.value} =
         ${input.isNull} ? ${CodeGenerator.defaultValue(dataType)} : $deserialize;
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
