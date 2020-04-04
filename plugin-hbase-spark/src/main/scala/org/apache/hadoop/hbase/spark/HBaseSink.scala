package org.apache.hadoop.hbase.spark

import org.apache.hadoop.hbase.client.Put
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * Created by Agile Lab s.r.l. on 04/11/2017.
  */
class HBaseSink(sparkSession: SparkSession, parameters: Map[String, String], hBaseContext: HBaseContext) extends Sink with Logging {

  /**
    * Non è gestito il commit log quindi un task può essere inserito due volte
    * Per creare questo metodo ho seguito il codice della libreria
    * elasticsearch-spark-20_2.11-6.0.0-rc1-sources.jar -> org.elasticsearch.spark.sql.streaming.EsSparkSqlStreamingSink
    *
    * @param batchId
    * @param data
    */
  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    val putConverterFactory = PutConverterFactory(parameters, data)
    val queryExecution = data.queryExecution
    val convertToPut: InternalRow => Put = putConverterFactory.convertToPut
    val hBaseContextInternal = hBaseContext
    SQLExecution.withNewExecutionId(sparkSession, queryExecution) {
      hBaseContextInternal
        .bulkPut(queryExecution.toRdd,
          putConverterFactory.getTableName(),
          convertToPut
        )
    }
  }

  /**
    * Execute a block of code, then a finally block, but if exceptions happen in
    * the finally block, do not suppress the original exception.
    *
    * This is primarily an issue with `finally { out.close() }` blocks, where
    * close needs to be called to clean up `out`, but if an exception happened
    * in `out.write`, it's likely `out` may be corrupted and `out.close` will
    * fail as well. This would then suppress the original/likely more meaningful
    * exception from the original `out.write` call.
    */
  def tryWithSafeFinally[T](block: => T)(finallyBlock: => Unit): T = {
    var originalThrowable: Throwable = null
    try {
      block
    } catch {
      case t: Throwable =>
        // Purposefully not using NonFatal, because even fatal exceptions
        // we don't want to have our finallyBlock suppress
        originalThrowable = t
        throw originalThrowable
    } finally {
      try {
        finallyBlock
      } catch {
        case t: Throwable =>
          if (originalThrowable != null) {
            originalThrowable.addSuppressed(t)
            logWarning(s"Suppressing exception in finally: " + t.getMessage, t)
            throw originalThrowable
          } else {
            throw t
          }
      }
    }
  }
}
