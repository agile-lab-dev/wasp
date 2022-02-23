package it.agilelab.bigdata.wasp.consumers.spark.plugins.plain.hbase.integration.sink

import it.agilelab.bigdata.wasp.consumers.spark.plugins.plain.hbase.integration.HBaseContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.{DataFrame, SparkSession}

class HBaseSink(sparkSession: SparkSession,
  parameters: Map[String, String],
  hBaseContext: HBaseContext) extends Sink with Logging {

  @volatile private var latestBatchId = -1L

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    if (batchId <= latestBatchId) {
      logInfo(s"Skipping already committed batch $batchId")
    } else {

      val queryExecution = data.queryExecution
      val hBaseContextInternal = hBaseContext
      val schema = data.schema

      logDebug(s"Start writing a new micro batch for schema $schema")

      SQLExecution.withNewExecutionId(sparkSession, queryExecution) {
        HBaseWriter.write(queryExecution, parameters, hBaseContextInternal, schema)
      }

      latestBatchId = batchId
    }
  }
}
