package it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr

import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.GdprStrategy.CorrelationId

/**
  * Represents the output result of the deletion process for a single key
 *
  * @param key Key that was requested to be handled
  * @param keyMatchType Type of match used to delete data about this key
  * @param source Source of data deleted
  * @param result Result of the deletion process
  * @param correlationId String that correlates multiple keys
  */
case class DeletionOutput(key: String,
                          keyMatchType: KeyMatchType,
                          source: DeletionSource,
                          result: DeletionResult,
                          correlationId: CorrelationId) {
  def toOutputDF: DeletionOutputDataFrame = {
    DeletionOutputDataFrame(
      key,
      keyMatchType.print,
      source.print,
      result.print,
      correlationId
    )
  }
}

object DeletionOutput {
  def apply(keyWithCorrelation: KeyWithCorrelation, keyMatchType: KeyMatchType, source: DeletionSource, result: DeletionResult): DeletionOutput = {
    new DeletionOutput(keyWithCorrelation.key, keyMatchType, source, result, keyWithCorrelation.correlationId)
  }
}
case class DeletionOutputDataFrame(key: String, keyMatchType: String, source: String, result: String, correlationId: CorrelationId)

sealed trait DeletionResult { def print: String }

case object DeletionSuccess extends DeletionResult {
  override def print = "SUCCESS"
}
case class DeletionFailure(exception: Throwable) extends DeletionResult {
  override def print = s"FAILURE: ${exception.getLocalizedMessage}"
}
case object DeletionNotFound extends DeletionResult {
  override def print = "NOT_FOUND"
}

sealed trait KeyMatchType { def print: String }

sealed trait HdfsMatchType extends KeyMatchType { val columnName: String }
case class HdfsExactColumnMatch(columnName: String) extends HdfsMatchType {
  override def print: String = s"EXACT_COLUMN|$columnName"
}
case class HdfsPrefixColumnMatch(columnName: String, matchedValues: Option[Seq[String]]) extends HdfsMatchType {
  override def print: String = s"PREFIX_COLUMN|$columnName" + matchedValues.fold("")(rows => s"|${rows.mkString(",")}")
}

sealed trait HBaseMatchType extends KeyMatchType
case object HBaseExactRowKeyMatch extends HBaseMatchType {
  override def print: String = "EXACT_ROWKEY"
}
case class HBasePrefixRowKeyMatch(matchedRows: Option[Seq[String]]) extends HBaseMatchType {
  override def print: String = "PREFIX_ROWKEY" + matchedRows.fold("")(rows => s"|${rows.mkString(",")}")
}
case class HBasePrefixWithTimeRowKeyMatch(matchedRows: Option[Seq[String]]) extends HBaseMatchType {
  override def print: String = "PREFIXWITHTIME_ROWKEY" + matchedRows.fold("")(rows => s"|${rows.mkString(",")}")
}

sealed trait DeletionSource { def print: String }
case class HdfsParquetSource(fileNames: Seq[String]) extends DeletionSource {
  override def print: String = s"HDFS|${fileNames.mkString(",")}"
}
case class HBaseTableSource(tableName: String) extends DeletionSource {
  override def print: String = s"HBASE|$tableName"
}
case object NoSourceFound extends DeletionSource {
  override def print: String = "NOT_FOUND"
}
