package it.agilelab.bigdata.wasp.consumers.spark.utils

import java.io.ByteArrayOutputStream

import it.agilelab.bigdata.wasp.consumers.spark.utils.EncodeUsingAvro.AvroSerializer
import it.agilelab.darwin.manager.AvroSchemaManager
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{BinaryEncoder, EncoderFactory}
import org.apache.spark.sql.catalyst.expressions.{Expression, NonSQLExpression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.types.{BinaryType, DataType}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._

case class EncodeUsingAvro[A](
    child: Expression,
    schema: String,
    avroSchemaManager: () => AvroSchemaManager,
    toGenericRecord: A => org.apache.avro.generic.GenericRecord
) extends UnaryExpression
    with NonSQLExpression {

  private lazy val serializer =
    new AvroSerializer[A](new Schema.Parser().parse(schema), avroSchemaManager(), toGenericRecord)

  override protected def nullSafeEval(input: Any): Any = {
    serializer.toSingleObjectEncoded(input.asInstanceOf[A])
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val serializerClass = classOf[EncodeUsingAvro.AvroSerializer[A]].getName
    // If a I add a reference to the schema instance I get a TaskNotSerializableException
    // therefore I opted for passing by the string representation, it should be done only
    // once since it is in the mutable variable initialization of the code-gen.
    // I saw that the same has been done in the others AvroSerializerExpressions
    val schemaStringRef = ctx.addReferenceObj("schemaStr", schema, "java.lang.String")
    val schemaClass     = classOf[Schema].getName
    val toGenericRecordRef =
      ctx.addReferenceObj("toGenericRecord", toGenericRecord, classOf[A => GenericRecord].getName)
    val schemaManagerClass = classOf[AvroSchemaManager].getName
    val schemaManagerFactoryRef =
      ctx.addReferenceObj("schemaManagerFactory", avroSchemaManager, classOf[() => AvroSchemaManager].getName)

    val schemaRef        = "schema"
    val schemaManagerRef = "schemaManager"

    ctx.addImmutableStateIfNotExists(
      schemaManagerClass,
      schemaManagerRef,
      v => s"$v = ($schemaManagerClass) $schemaManagerFactoryRef.apply();"
    )
    ctx.addImmutableStateIfNotExists(
      schemaClass,
      schemaRef,
      v => s"$v = new $schemaClass.Parser().parse($schemaStringRef);"
    )

    val serializer = ctx.addMutableState(
      serializerClass,
      "serializer",
      v => s"$v = new $serializerClass($schemaRef, $schemaManagerRef, $toGenericRecordRef);"
    )

    // Code to serialize. this code is copy pasted from kryo expression encoder
    val input     = child.genCode(ctx)
    val javaType  = CodeGenerator.javaType(dataType)
    val serialize = s"$serializer.toSingleObjectEncoded(${input.value})"

    val code = input.code +
      code"""final $javaType ${ev.value} =
        ${input.isNull} ? ${CodeGenerator.defaultValue(dataType)} : $serialize;
     """
    ev.copy(code = code, isNull = input.isNull)
  }

  override def dataType: DataType = BinaryType

}

object EncodeUsingAvro {

  /**
    * Stateful avro serializer: NOT thread safe
    */
  final private class AvroSerializer[A](
      schema: Schema,
      avroSchemaManager: AvroSchemaManager,
      toRecord: A => GenericRecord
  ) {

    private[this] val fingerprint                               = avroSchemaManager.getId(schema)
    private[this] val outputStream: ByteArrayOutputStream       = new ByteArrayOutputStream()
    private[this] var encoder: BinaryEncoder                    = _ // scalastyle:ignore
    private[this] val writer: GenericDatumWriter[GenericRecord] = new GenericDatumWriter[GenericRecord](schema)

    def toSingleObjectEncoded(obj: A): Array[Byte] = {
      outputStream.reset()
      encoder = EncoderFactory.get().binaryEncoder(outputStream, encoder)
      avroSchemaManager.writeHeaderToStream(outputStream, fingerprint)
      writer.write(toRecord(obj), encoder)
      encoder.flush()
      outputStream.toByteArray
    }
  }

}
