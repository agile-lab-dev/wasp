package it.agilelab.bigdata.wasp.consumers.spark.strategies.cdc

import it.agilelab.bigdata.wasp.consumers.spark.strategies.{ReaderKey, Strategy}
import it.agilelab.bigdata.wasp.core.logging.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class DebeziumMutationStrategy extends Strategy with Logging {

  /**
    *
    * @param dataFrames
    * @return
    */
  override def transform(dataFrames: Map[ReaderKey, DataFrame]): DataFrame = {

    logger.info(s"Strategy configuration: $configuration")

    val df = dataFrames.head._2

    DebeziumConversion.conversion(
      df
    )
  }
}

case object DebeziumConversion extends CdcMapper {
  override def BEFORE: String = "before"

  override def AFTER: String = "after"

  override def OPERATION: String = "op"

  override def TIMESTAMP: String = "ts_ms"

  override def COMMIT_ID: String = "commitId"

  override def PRIMARY_KEY: String = "key"

  override def conversionToCdcFormat(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select("kafkaMetadata.key", BEFORE, AFTER, OPERATION, TIMESTAMP)
      .withColumn(COMMIT_ID, lit(0))
  }
}
