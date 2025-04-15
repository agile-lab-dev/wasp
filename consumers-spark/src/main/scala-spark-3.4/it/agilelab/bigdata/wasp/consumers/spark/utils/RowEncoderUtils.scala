package it.agilelab.bigdata.wasp.consumers.spark.utils

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Encoder, Row}

object RowEncoderUtils {
  def encoderFor(schema: StructType): Encoder[Row] = {
    RowEncoder.apply(schema)
  }

}
