package it.agilelab.bigdata.wasp.consumers.spark.utils

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Encoder, Row}

object RowEncoderUtils {
  def encoderFor(schema: StructType): Encoder[Row] = {
    // here we can't use `RowEncoder.encoderFor(schema)`
    // because otherwise org.apache.spark.sql.catalyst.plans.logical.CatalystSerde.generateObjAttr
    // will throw:
    //  org.apache.spark.SparkRuntimeException: Only expression encoders are supported for now.
    ExpressionEncoder.apply(schema)
  }

}
