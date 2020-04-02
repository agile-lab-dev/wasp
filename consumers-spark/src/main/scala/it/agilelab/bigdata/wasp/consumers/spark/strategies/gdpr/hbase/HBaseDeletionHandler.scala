package it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.hbase

import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr._
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.config.HBaseDeletionConfig
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.hbase.HBaseDeletionHandler.RowKeyMatched
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.utils.GdprUtils
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.utils.hbase.HBaseUtils
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.utils.GdprUtils._
import it.agilelab.bigdata.wasp.consumers.spark.utils.HBaseConnection
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.configuration.HBaseConfigModel
import it.agilelab.bigdata.wasp.core.models.{ExactKeyValueMatchingStrategy, KeyValueMatchingStrategy, PrefixAndTimeBoundKeyValueMatchingStrategy, PrefixKeyValueMatchingStrategy}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.hbase.client.{Scan, Table}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

object HBaseDeletionHandler extends Logging {

  type KeyToMatch = Array[Byte]
  type RowKeyMatched = Array[Byte]

  def delete(config: HBaseDeletionConfig): Try[Seq[DeletionOutput]] = {
    logger.info("Starting HBase deletion handling")
    val output = Try(delete(config.tableName, config.hbaseConfigModel, config.keysWithScan, config.keyValueMatchingStrategy))
    output match {
      case Failure(_) => logger.info("Deletion failed")
      case Success(_) => logger.info("Deletion completed successfully")
    }
    output
  }

  def delete(tableName: String,
             hbaseConfig: Option[HBaseConfigModel],
             keysWithScanRDD: RDD[(KeyWithCorrelation, Scan)],
             keyValueMatchingStrategy: KeyValueMatchingStrategy): Seq[DeletionOutput] = {
    keysWithScanRDD.mapPartitions { keysWithScan: Iterator[(KeyWithCorrelation, Scan)] =>
      val hBaseConnection = new HBaseConnection(hbaseConfig)
      TaskContext.get().addTaskCompletionListener(_ => hBaseConnection.closeConnection())
      hBaseConnection.withTable(tableName) { table =>
        keyValueMatchingStrategy match {
          case _: ExactKeyValueMatchingStrategy =>
            keysWithScan.map { case (keyWithCorrelation, scan) =>
              deleteRowKey(table)(scan, keyWithCorrelation.key.asRowKey) match {
                case Failure(exception) => createOutput(table, keyWithCorrelation, HBaseExactRowKeyMatch, DeletionFailure(exception))
                case Success(result) => createOutput(table, keyWithCorrelation, HBaseExactRowKeyMatch, result)
              }
            }
          case _: PrefixKeyValueMatchingStrategy =>
            keysWithScan.map { case (key, scan) =>
              deleteMultipleRowKeys(table)(scan) match {
                case Failure(exception) => createOutput(table, key, HBasePrefixRowKeyMatch(None), DeletionFailure(exception))
                case Success(MultipleDeletionResult(rowKeysMatched, result)) =>
                  createOutput(table, key, HBasePrefixRowKeyMatch(rowKeysMatched.map(_.map(_.asString))), result)
              }
            }
          case _: PrefixAndTimeBoundKeyValueMatchingStrategy =>
            keysWithScan.map { case (key, scan) =>
              deleteMultipleRowKeys(table)(scan) match {
                case Failure(exception) => createOutput(table, key, HBasePrefixWithTimeRowKeyMatch(None), DeletionFailure(exception))
                case Success(MultipleDeletionResult(rowKeysMatched, result)) =>
                  createOutput(table, key, HBasePrefixWithTimeRowKeyMatch(rowKeysMatched.map(_.map(_.asString))), result)
              }
            }
        }
      }
    }.collect()
  }

  /* Returns true if `keyToMatch` is returned from the provided `scan`, else it returns false */
  private def searchSingleRowKey(table: Table)(keyToMatch: KeyToMatch, scan: Scan): Try[Boolean] = Try {
    val scanner = table.getScanner(scan)
    try {
      val iter = scanner.iterator()
      if (iter.hasNext) {
        val rowKeyMatched = iter.next().getRow
        if (rowKeyMatched.equals(keyToMatch)) {
          true
        }
        else {
          throw new IllegalStateException(s"RowKey found '${rowKeyMatched.asString}' is different from key to delete '${keyToMatch.asString}'")
        }
      }
      else {
        false
      }
    } finally {
      IOUtils.closeQuietly(scanner)
    }
  }

  /* Returns the rowKeys matched from the provided `scan` */
  private def searchAndReturnKeys(table: Table)(scan: Scan): Try[Seq[RowKeyMatched]] = Try {
    val scanner = table.getScanner(scan)
    try {
      val iter = scanner.iterator()
      val buf = new ListBuffer[Array[Byte]]()
      while (iter.hasNext) {
        buf += iter.next().getRow
      }
      buf
    } finally {
      IOUtils.closeQuietly(scanner)
    }
  }

  /* Searches the rowKey using the provided Scan, and if exists deletes it */
  private def deleteRowKey(table: Table)(scan: Scan, keyToMatch: KeyToMatch): Try[DeletionResult] = {
    for {
      rowKeyExists <- searchSingleRowKey(table)(keyToMatch, scan)
      deletionResult <- if (rowKeyExists) {
        HBaseUtils.deleteRow(table)(keyToMatch).map(_ => DeletionSuccess)
      } else {
        Success(DeletionNotFound)
      }
    } yield deletionResult
  }

  /* Searches for multiple matches of the provided Scan, and deletes each of them */
  private def deleteMultipleRowKeys(table: Table)(scan: Scan): Try[MultipleDeletionResult] = {
    val tryRowKeysDeleted = for {
      rowKeysFound <- searchAndReturnKeys(table)(scan)
      rowKeysDeleted <- GdprUtils.traverseWithTry(rowKeysFound) { row =>
        HBaseUtils.deleteRow(table)(row).map(_ => row)
      }
    } yield rowKeysDeleted

    tryRowKeysDeleted.map { rowKeysMatched =>
      if (rowKeysMatched.isEmpty) {
        MultipleDeletionResult(None, DeletionNotFound)
      } else {
        MultipleDeletionResult(Some(rowKeysMatched), DeletionSuccess)
      }
    }
  }

  private def createOutput(table: Table, keyWithCorrelation: KeyWithCorrelation, keyMatchType: KeyMatchType, result: DeletionResult) = {
    DeletionOutput(keyWithCorrelation.key, keyMatchType, HBaseTableSource(table.getName.getNameAsString), result, keyWithCorrelation.correlationId)
  }


}

case class MultipleDeletionResult(rowKeysMatched: Option[Seq[RowKeyMatched]], result: DeletionResult)