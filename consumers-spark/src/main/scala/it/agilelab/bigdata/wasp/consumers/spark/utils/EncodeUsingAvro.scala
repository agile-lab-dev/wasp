package it.agilelab.bigdata.wasp.consumers.spark.utils

import java.io.ByteArrayOutputStream

import it.agilelab.darwin.manager.util.AvroSingleObjectEncodingUtils
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{BinaryEncoder, EncoderFactory}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, NonSQLExpression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{BinaryType, DataType}

case class EncodeUsingAvro[A](
                               child: Expression,
                               schema: String,
                               toGenericRecord: A => org.apache.avro.generic.GenericRecord
                             ) extends UnaryExpression
  with NonSQLExpression {

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported")

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val serializer = ctx.freshName("serializer")
    val serializerClass = classOf[EncodeUsingAvro.AvroSerializer[A]].getName
    // If a I add a reference to the schema instance I get a TaskNotSerializableException
    // therefore I opted for passing by the string representation, it should be done only
    // once since it is in the mutable variable initialization of the code-gen.
    // I saw that the same has been done in the others AvroSerializerExpressions
    val schemaStringRef = ctx.addReferenceObj("schemaStr", schema, "java.lang.String")
    val schemaRef = ctx.freshName("schema")
    val schemaClass = classOf[Schema].getName
    val toGenericRecordRef =
      ctx.addReferenceObj("toGenericRecord", toGenericRecord, classOf[A => GenericRecord].getName)

    val serializerInit =
      s"""
           $schemaClass $schemaRef = new $schemaClass.Parser().parse($schemaStringRef);
           $serializer = new $serializerClass($schemaRef, $toGenericRecordRef);"""

    ctx.addMutableState(serializerClass, serializer, serializerInit)

    // Code to serialize. this code is copy pasted from kryo expression encoder
    val input = child.genCode(ctx)
    val javaType = ctx.javaType(dataType)
    val serialize = s"$serializer.toSingleObjectEncoded(${input.value})"

    val code =
      s"""
      ${input.code}
      final $javaType ${ev.value} = ${input.isNull} ? ${ctx.defaultValue(javaType)} : $serialize;
     """
    ev.copy(code = code, isNull = input.isNull)
  }

  override def dataType: DataType = BinaryType

}
object EncodeUsingAvro {
  /**
    * Stateful avro serializer: NOT thread safe
    */
  private final class AvroSerializer[A](schema: Schema, toRecord: A => GenericRecord) {

    private[this] val fingerprint = AvroSingleObjectEncodingUtils.getId(schema)
    private[this] val outputStream: ByteArrayOutputStream = new ByteArrayOutputStream()
    private[this] var encoder: BinaryEncoder = _ // scalastyle:ignore
    private[this] val writer: GenericDatumWriter[GenericRecord] = new GenericDatumWriter[GenericRecord](schema)

    def toSingleObjectEncoded(obj: A): Array[Byte] = {
      outputStream.reset()
      encoder = EncoderFactory.get().binaryEncoder(outputStream, encoder)
      AvroSingleObjectEncodingUtils.writeHeaderToStream(outputStream, fingerprint)
      writer.write(toRecord(obj), encoder)
      encoder.flush()
      outputStream.toByteArray
    }
  }

}
