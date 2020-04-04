package it.agilelab.bigdata.wasp.consumers.spark.strategies

import it.agilelab.bigdata.wasp.consumers.spark.strategies.{ReaderKey, Strategy}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{array, lit, struct}

/**
  * A simple strategy that drops the kafkaMetadata column.
  */
class DropKafkaMetadata extends Strategy {
  override def transform(dataFrames: Map[ReaderKey, DataFrame]): DataFrame = {
    val df = dataFrames.head._2
    df.drop("kafkaMetadata")
  }
}
