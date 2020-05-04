package it.agilelab.bigdata.wasp.consumers.spark.strategies

import java.time.Instant

import it.agilelab.bigdata.wasp.consumers.spark.strategies.{ReaderKey, Strategy}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{array, lit, struct, to_json}

/**
  * A simple strategy that drops the kafkaMetadata column.
  */
class DropKafkaMetadata extends Strategy {
  override def transform(dataFrames: Map[ReaderKey, DataFrame]): DataFrame = {
    val df = dataFrames.head._2
    df.drop("kafkaMetadata")
  }
}




