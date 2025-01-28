package it.agilelab.bigdata.wasp.consumers.spark.plugins.postgresql

import it.agilelab.bigdata.wasp.consumers.spark.writers.SparkStructuredStreamingWriter
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.models.SQLSinkModel
import org.apache.spark.TaskContext
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{DataFrame, Row}

import scala.util.{Failure, Success}

/**
	* A [[SparkStructuredStreamingWriter]] that writes to PostgreSQL using upserts (INSERT ON CONFLICT)
	*
	* @param sqlSinkModel  Model for the upsert operations
  */
case class PostgreSQLSparkStructuredStreamingWriter(override val sqlSinkModel: SQLSinkModel)
    extends PostgreSQLSparkBaseWriter
    with SparkStructuredStreamingWriter {

  import PostgreSQLSparkStructuredStreamingWriter.log

  override def write(stream: DataFrame): DataStreamWriter[Row] = {
    log.info(s"Creating writer")
    val schema = stream.schema

    PostgreSQLConsumerSparkPlugin.validateSQLSinkModelAgainstSchema(schema, sqlSinkModel)

    val dsw = stream.writeStream
      .foreachBatch { (df: DataFrame, batchId: Long) =>
        log.info(s"Starting write operation for batch $batchId")

        val metadata = fetchMetadata(sqlSinkModel.table)

        df.foreachPartition { rows: Iterator[Row] =>
          val taskContext = TaskContext.get()
          val taskId      = taskContext.taskAttemptId()
          val writeId     = PostgreSQLUpsertWriter.writeId(batchId, taskId)

          log.info(s"Write operation for $writeId preparing")
          val connection = createConnection()
          taskContext.addTaskCompletionListener[Unit] { _: TaskContext =>
            log.info(s"Write operation for $writeId closing connections")
            if (!connection.isClosed) connection.close()
          }
          val writer = new PostgreSQLUpsertWriter(sqlSinkModel, schema, metadata)

          log.info(s"Write operation for $writeId starting")
          val writeOutcome = writer.write(rows, connection, writeId)
          writeOutcome match {
            case Success(_) => log.info(s"Write operation for $writeId completed")
            case Failure(exception) =>
              log.error(s"Write operation for $writeId failed", exception)
              throw exception
          }

          connection.close()
        }
        log.info(s"Write operation for batch $batchId completed")
        () // for compatibility reason between scala versions
      }

    log.info(s"Done creating writer")

    dsw
  }

}

object PostgreSQLSparkStructuredStreamingWriter extends Logging {
  // this only exists to keep the logger in the companion object for serializability
  def log = logger
}
