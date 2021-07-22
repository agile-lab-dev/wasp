package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.model

sealed trait WriterDetails
object WriterDetails {
  val parallelWrite = "parallelWrite"
  val continuousUpdate = "continuousUpdate"
}

/**
 * Details needeed by parallel writer
 * @param saveMode spark save mode
 * @param partitionBy partition columns
 */
case class ParallelWrite(saveMode: String, partitionBy: Option[List[String]]) extends WriterDetails

/**
 * Details needed by continuous update writer
 * @param tableName delta table name
 * @param keys delta table unique keys column list
 * @param orderingExpression monotonically increasing select expression to choose upsert candidate
 * @param fieldsToDrop columns used in orderingExpression but not needed in delta table
 */
case class ContinuousUpdate(tableName: String,
                            keys: List[String],
                            orderingExpression: String,
                            fieldsToDrop: List[String]) extends WriterDetails

