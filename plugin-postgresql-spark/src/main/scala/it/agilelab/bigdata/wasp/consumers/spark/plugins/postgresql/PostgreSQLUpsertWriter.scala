package it.agilelab.bigdata.wasp.consumers.spark.plugins.postgresql

import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.models.SQLSinkModel
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

import java.sql.{Connection, Date, PreparedStatement, SQLException, Time, Timestamp}
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class PostgreSQLUpsertWriter(sqlSinkModel: SQLSinkModel, schema: StructType, metadata: TableMetadata)
    extends PostgreSQLUpsertGenerationSupport
    with Logging {

  def write(rows: Iterator[Row], connection: Connection, writeId: String): Try[Long] =
    for {
      upsertStatement  <- createUpsertStatement(connection, schema, writeId)
      writeRowsOutcome <- writeRows(rows, upsertStatement, metadata, connection, writeId)
    } yield writeRowsOutcome

  private def createUpsertStatement(
      connection: Connection,
      schema: StructType,
      writeId: String
  ): Try[PreparedStatement] = Try {
    val upsertQuery = generateUpsertQuery(sqlSinkModel, schema)
    logger.debug(s"Write operation for $writeId upsert query: \n" + upsertQuery)
    connection.prepareStatement(upsertQuery)
  }

  private def writeRows(
      rows: Iterator[Row],
      upsertStatement: PreparedStatement,
      metadata: TableMetadata,
      connection: Connection,
      writeId: String
  ): Try[Long] = Try {
    logger.info(s"Write operation for $writeId starting to write rows")
    val numRows = rows
      .grouped(sqlSinkModel.batchSize)
      .zipWithIndex
      .map {
        case (rowGroup, rowGroupIndex) =>
          val rowGroupSize = rowGroup.size
          logger.debug(s"Write operation for $writeId row group $rowGroupIndex of $rowGroupSize rows starting")

          logger.debug(s"Write operation for $writeId row group $rowGroupIndex adding rows to statement batch")
          upsertStatement.clearBatch()
          rowGroup.foreach(row => addRowToStatementBatch(row, metadata, upsertStatement))

          logger.debug(s"Write operation for $writeId row group $rowGroupIndex executing statement")
          val tryUpdateCounts = Try(upsertStatement.executeBatch())
          if (tryUpdateCounts.isFailure) {
            connection.rollback()
            rowGroupSize
          } else {
            checkUpdateCounts(rowGroup, writeId, rowGroupIndex, tryUpdateCounts)

            logger.debug(s"Write operation for $writeId row group $rowGroupIndex committing")
            connection.commit()
            upsertStatement.clearBatch()

            logger.debug(s"Write operation for $writeId row group $rowGroupIndex of $rowGroupSize rows finished")
            rowGroupSize
          }
      }
      .sum
    logger.info(s"Write operation for $writeId finished writing $numRows rows total")
    numRows
  }

  private def addRowToStatementBatch(row: Row, metadata: TableMetadata, preparedStatement: PreparedStatement): Unit = {
    row.toSeq.zipWithIndex
      .foreach {
        case (value, index) =>
          setParameter(
            preparedStatement,
            metadata,
            index + 1, // PreparedStatement's indices are 1-based, so add 1
            value
          )
      }
    preparedStatement.addBatch()
  }

  private def setParameter(
      statement: PreparedStatement,
      metadata: TableMetadata,
      oneBasedIndex: Int,
      value: Any
  ): Unit = {
    if (value == null) {
      val zeroBasedIndex = oneBasedIndex - 1
      statement.setNull(oneBasedIndex, metadata.columnIndexToMetadata(zeroBasedIndex).typeNumber)
    } else {
      setNonNullParameter(statement, oneBasedIndex, value)
    }
  }

  private def setNonNullParameter(statement: PreparedStatement, index: Int, value: Any): Unit = {
    value match {
      case boolean: Boolean       => statement.setBoolean(index, boolean)
      case byte: Byte             => statement.setByte(index, byte)
      case short: Short           => statement.setInt(index, short)
      case int: Int               => statement.setInt(index, int)
      case long: Long             => statement.setLong(index, long)
      case float: Float           => statement.setFloat(index, float)
      case double: Double         => statement.setDouble(index, double)
      case bigDecimal: BigDecimal => statement.setBigDecimal(index, bigDecimal.bigDecimal)
      case char: Char             => statement.setString(index, char.toString)
      case string: String         => statement.setString(index, string)
      case byteArray: Array[Byte] => statement.setBytes(index, byteArray)
      case timestamp: Timestamp   => statement.setTimestamp(index, timestamp)
      case date: Date             => statement.setDate(index, date)
      case time: Time             => statement.setTime(index, time)
      // TODO support array types
      case _ =>
        throw new IllegalArgumentException(
          s"""Can't translate non-null value "$value" for parameter at position $index"""
        )
    }
  }

  private def checkUpdateCounts(
      rowGroup: Seq[Row],
      writeId: String,
      rowGroupIndex: Int,
      tryUpdateCounts: Try[Array[Int]]
  ): Unit = {
    tryUpdateCounts match {
      case Success(updateCounts) =>
        if (updateCounts.exists(_ < 0)) {
          // at least one row failed without an exception being thrown
          // can happen if the JDBC driver is setup to keep processing a batch even if an update fails
          // find the failed rows, log them, and throw an exception
          val failedUpdates = updateCounts.toList.zipWithIndex
            .zip(rowGroup)
            .map { case ((updateCount, index), row) => (updateCount, index, row) }
            .filter(_._1 < 0)
          failedUpdates
            .foreach {
              case (updateCount, index, row) =>
                val exception = new SQLException(
                  s"Write operation for $writeId row group $rowGroupIndex encountered an issue: row $row at position $index returned negative update value $updateCount"
                )
                logger.error(exception.getMessage, exception)
            }

          throw new SQLException(
            s"Write operation for $writeId row group $rowGroupIndex encountered an issue: a total of ${failedUpdates.size} row updates failed in row group $rowGroupIndex"
          )
        }
      case Failure(throwable) =>
        // the batch failed, grab the chained SQLExceptions and nest them into a single one for ease of handling
        val sqlException = throwable.asInstanceOf[SQLException]
        val nestedExceptions = sqlException.iterator().asScala.reduce[Throwable] {
          case (ex, cause) =>
            val t = new Exception(ex.getMessage, cause)
            t.setStackTrace(ex.getStackTrace)
            t
        }
        val wrappedException = new SQLException(
          s"Write operation for $writeId row group $rowGroupIndex encountered an issue",
          nestedExceptions
        )
        logger.error(wrappedException.getMessage, wrappedException)

        throw wrappedException
    }
  }
}

object PostgreSQLUpsertWriter {
  def writeId(batchId: Long, taskId: Long): String = s"batch $batchId task $taskId"
  def writeId(taskId: Long): String                = s"task $taskId"
}
