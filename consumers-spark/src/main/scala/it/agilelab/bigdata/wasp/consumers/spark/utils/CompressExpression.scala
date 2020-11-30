package it.agilelab.bigdata.wasp.consumers.spark.utils

import java.io.{ ByteArrayOutputStream, OutputStream }

import CompressExpression._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.{ CompressionCodec, CompressionCodecFactory }
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{ CodeGenerator, CodegenContext, ExprCode }
import org.apache.spark.sql.catalyst.expressions.{ ExpectsInputTypes, Expression, UnaryExpression }
import org.apache.spark.sql.types.{ BinaryType, DataType }

object CompressExpression {
  def compress(col: Column, codec: String, conf: Configuration): Column =
    new Column(CompressExpression(codec, new HadoopConfiguration(conf), col.expr))

  private def wrapException(s: String): String =
    s"""try {
       |  $s
       |} catch (Exception e) {
       |  throw new RuntimeException(e);
       |}
       |""".stripMargin

  // region constants

  private val bufferClassName  = classOf[ByteArrayOutputStream].getCanonicalName
  private val outputStreamType = classOf[OutputStream].getCanonicalName
  private val byteArrayType    = CodeGenerator.javaType(BinaryType)
  private val factoryType      = classOf[CompressionCodecFactory].getCanonicalName
  private val codecType        = classOf[CompressionCodec].getCanonicalName
  private val codecVarName     = "codec"
  private val factoryVarName   = "factory"

  // endregion

}

case class CompressExpression(codecName: String, conf: HadoopConfiguration, _child: Expression)
    extends UnaryExpression
    with ExpectsInputTypes {

  override def child: Expression = _child

  @transient
  private lazy val factory = new CompressionCodecFactory(conf.value)
  @transient
  private lazy val codec   = factory.getCodecByName(codecName)

  override lazy val deterministic: Boolean = child.deterministic

  override protected def nullSafeEval(input: Any): Any = {
    val bos    = new ByteArrayOutputStream()
    val stream = codec.createOutputStream(bos)
    stream.write(input.asInstanceOf[Array[Byte]])
    stream.flush()
    stream.close()
    bos.toByteArray
  }

  override def prettyName: String = "compress"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val hConfVarName =
      ctx.addReferenceObj("hadoopConf", conf, classOf[HadoopConfiguration].getCanonicalName)
    val bufferName   = ctx.addMutableState(bufferClassName, "buffer", name => s"$name = new $bufferClassName();")

    ctx.addImmutableStateIfNotExists(
      javaType = factoryType,
      variableName = factoryVarName,
      (v: String) => wrapException(s"""$v = new $factoryType($hConfVarName.value());""")
    )
    ctx.addImmutableStateIfNotExists(
      javaType = codecType,
      variableName = codecVarName,
      (v: String) => wrapException(s"""$v = $factoryVarName.getCodecByName("$codecName");""")
    )

    val input = child.genCode(ctx)
    val code  =
      code"""${input.code}
            |$byteArrayType ${ev.value} = null;
            |if (${input.isNull}) {
            |  ${ev.value} = null;
            |} else {
            |  try {
            |    $bufferName.reset();
            |    $outputStreamType stream = codec.createOutputStream($bufferName);
            |    stream.write(${input.value});
            |    stream.flush();
            |    stream.close();
            |    ${ev.value} = ($byteArrayType) $bufferName.toByteArray();
            |  } catch (Exception e) {
            |    throw new RuntimeException(e);
            |  }
            |}""".stripMargin
    ev.copy(code = code, isNull = input.isNull)
  }

  override def inputTypes: Seq[BinaryType] = Seq(BinaryType)

  override def dataType: DataType = BinaryType
}
