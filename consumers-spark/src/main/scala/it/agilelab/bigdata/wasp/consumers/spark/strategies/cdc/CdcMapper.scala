package it.agilelab.bigdata.wasp.consumers.spark.strategies.cdc

import it.agilelab.bigdata.wasp.models.GenericCdcMutationFields
import org.apache.spark.sql.DataFrame

/**
  * trait used to correctly translate the format
  * from the source format to the delta writer expected format.
  * For a more in depth understanding of the destination writer
  *
  * @see it.agilelab.bigdata.wasp.consumers.spark.plugins.cdc.DeltaLakeWriter
  */
private[cdc] trait CdcMapper {

  def BEFORE: String

  def AFTER: String

  def OPERATION: String

  def TIMESTAMP: String

  def COMMIT_ID: String

  def PRIMARY_KEY: String

  def conversion(
      dataFrame: DataFrame
  ): DataFrame = {
    import org.apache.spark.sql.functions.{col, struct}
    val df = conversionToCdcFormat(dataFrame)

    df.select(
        col(BEFORE).as(GenericCdcMutationFields.BEFORE_IMAGE),
        col(AFTER).as(GenericCdcMutationFields.AFTER_IMAGE),
        col(OPERATION).as(GenericCdcMutationFields.TYPE),
        col(TIMESTAMP).as(GenericCdcMutationFields.TIMESTAMP),
        col(COMMIT_ID).as(GenericCdcMutationFields.COMMIT_ID),
        col(PRIMARY_KEY).as(GenericCdcMutationFields.PRIMARY_KEY)
      )
      .withColumn(
        "value",
        struct(
          col(GenericCdcMutationFields.BEFORE_IMAGE),
          col(GenericCdcMutationFields.AFTER_IMAGE),
          col(GenericCdcMutationFields.TYPE),
          col(GenericCdcMutationFields.TIMESTAMP),
          col(GenericCdcMutationFields.COMMIT_ID)
        )
      )
      .select(
        col("key"),
        col("value")
      )
  }

  protected def conversionToCdcFormat(dataFrame: DataFrame): DataFrame
}

/**
  * Object used to identify and map the type of operations and the
  * values used to represent the types of operation in the source mutation.
  */
private[cdc] trait Operation {
  type OperationType = String

  def insert: OperationType

  def update: OperationType

  def delete: OperationType

  def truncate: OperationType
}
