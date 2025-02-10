package it.agilelab.bigdata.wasp.consumers.spark.utils

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.StructType

import scala.reflect.ClassTag

trait CompatibilityEncoders {
  def expressionEncoder[A](serializer: Seq[EncodeUsingAvro[A]],
                        deserializer: DecodeUsingAvro[A],
                        clsTag: ClassTag[A],
                        schema: StructType,
                        flat: Boolean
                       ):Encoder[A] =
  ExpressionEncoder[A](
    objSerializer = serializer.head,
    objDeserializer = deserializer,
    clsTag = clsTag
  )
}
