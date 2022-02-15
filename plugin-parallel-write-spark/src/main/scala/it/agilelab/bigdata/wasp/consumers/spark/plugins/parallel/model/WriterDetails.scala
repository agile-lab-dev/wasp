package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.model

sealed trait WriterDetails
object WriterDetails {
  val parallelWrite = "parallelWrite"
  val continuousUpdate = "continuousUpdate"
}

/**
 * Details needeed by parallel writer
 * @param saveMode spark save mode
 */
case class ParallelWrite(saveMode: String) extends WriterDetails

/**
 * Details needed by continuous update writer
 * @param keys delta table unique keys column list
 * @param orderingExpression monotonically increasing select expression to choose upsert candidate
 */
case class ContinuousUpdate(keys: List[String], orderingExpression: String) extends WriterDetails

