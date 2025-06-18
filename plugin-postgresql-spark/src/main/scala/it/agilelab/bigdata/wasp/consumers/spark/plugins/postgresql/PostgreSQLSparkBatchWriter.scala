package it.agilelab.bigdata.wasp.consumers.spark.plugins.postgresql

import it.agilelab.bigdata.wasp.consumers.spark.writers.SparkBatchWriter
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.models.SQLSinkModel
import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, Row}

import scala.util.{Failure, Success}

/** A [[SparkBatchWriter]] that writes to PostgreSQL using upserts (INSERT ON CONFLICT)
  *
  * @param sqlSinkModel
  *   Model for the upsert operations
  */
case class PostgreSQLSparkBatchWriter(override val sqlSinkModel: SQLSinkModel)
    extends PostgreSQLSparkBaseWriter
    with SparkBatchWriter {

  import PostgreSQLSparkBatchWriter.log

  override def write(df: DataFrame): Unit = {
    log.info(s"Starting write operation")

    val schema = df.schema
    PostgreSQLConsumerSparkPlugin.validateSQLSinkModelAgainstSchema(schema, sqlSinkModel)
    val metadata = fetchMetadata(sqlSinkModel.table)

    df.foreachPartition { rows: Iterator[Row] =>
      val taskContext = TaskContext.get()
      val taskId      = taskContext.taskAttemptId()
      val writeId     = PostgreSQLUpsertWriter.writeId(taskId)

      log.info(s"Write operation for $writeId preparing")
      val connection = createConnection()
      taskContext.addTaskCompletionListener[Unit] { _ =>
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

    log.info(s"Write operation completed")
  }

}

object PostgreSQLSparkBatchWriter extends Logging {
  // this only exists to keep the logger in the companion object for serializability
  def log = logger
}
