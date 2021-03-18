package it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test

import it.agilelab.bigdata.wasp.consumers.spark.plugins.cdc.GenericMutationFields
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
      df,
      DebeziumConversion.BEFORE,
      DebeziumConversion.AFTER,
      DebeziumConversion.OPERATION,
      DebeziumConversion.TIMESTAMP
    )

  }
}

sealed trait CdcConversion {
  def conversion(
      dataFrame: DataFrame,
      beforeField: String,
      afterField: String,
      operationField: String,
      timestampField: String
  ): DataFrame = {
    val df = conversionToCdcFormat(dataFrame)
    val fixedFlat = df
      .withColumnRenamed(beforeField, GenericMutationFields.BEFORE_IMAGE)
      .withColumnRenamed(afterField, GenericMutationFields.AFTER_IMAGE)
      .withColumnRenamed(operationField, GenericMutationFields.TYPE)
      .withColumnRenamed(timestampField, GenericMutationFields.TIMESTAMP)

    fixedFlat.selectExpr(
      "key",
      s"struct(" +
        s"${GenericMutationFields.BEFORE_IMAGE}, " +
        s"${GenericMutationFields.AFTER_IMAGE}, " +
        s"${GenericMutationFields.TYPE}, " +
        s"${GenericMutationFields.TIMESTAMP}, " +
        s"${GenericMutationFields.COMMIT_ID}) as value"
    )
  }

  protected def conversionToCdcFormat(dataFrame: DataFrame): DataFrame
}

case object DebeziumConversion extends CdcConversion {

  val BEFORE    = "before"
  val AFTER     = "after"
  val SOURCE    = "source"
  val OPERATION = "op"
  val TIMESTAMP = "ts_ms"
  val COMMIT_ID = "commitId"

  override def conversionToCdcFormat(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select("kafkaMetadata.key", BEFORE, AFTER, SOURCE, OPERATION, TIMESTAMP)
      .drop(SOURCE)
      .withColumn(COMMIT_ID, lit(0))

  }
}
