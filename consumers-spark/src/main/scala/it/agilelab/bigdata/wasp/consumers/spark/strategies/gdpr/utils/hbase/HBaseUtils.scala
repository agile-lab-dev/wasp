package it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.utils.hbase

import java.nio.charset.StandardCharsets

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Delete, Table}

import scala.util.Try

object HBaseUtils {

  def getTable(connection: Connection)(tableName: String): Try[Table] = {
    Try(connection.getTable(TableName.valueOf(tableName)))
  }

  def deleteRow(table: Table)(rowKey: Array[Byte]): Try[Unit] = {
    Try(table.delete(new Delete(rowKey)))
  }

  implicit class RowKeyToString(rowKey: Array[Byte]) {
    def asString: String = new String(rowKey, StandardCharsets.UTF_8)
  }

  implicit class StringToRowKey(string: String) {
    def asRowKey: Array[Byte] = string.getBytes(StandardCharsets.UTF_8)
  }

}
