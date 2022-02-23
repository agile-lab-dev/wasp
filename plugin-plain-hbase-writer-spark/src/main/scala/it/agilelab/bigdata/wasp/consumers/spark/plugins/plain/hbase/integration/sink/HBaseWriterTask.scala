package it.agilelab.bigdata.wasp.consumers.spark.plugins.plain.hbase.integration.sink

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.BinaryType

object HBaseWriterTask extends Serializable with Logging {

  import HBaseWriterProperties._

  def mutate(iterator: Iterator[InternalRow],
      tableName: TableName,
      connection: Connection,
      fieldIndexes: Map[String, Int], batchSize: Int): Unit = {
    val table = connection.getTable(tableName)
    val mutationList = new java.util.ArrayList[Mutation]

    iterator.foreach { row =>
      mutationList.add(createMutation(row, fieldIndexes))

      if (mutationList.size >= batchSize) {
        executeBatch(table, mutationList)
      }
    }

    if (mutationList.size() > 0) {
      executeBatch(table, mutationList)
    }

    table.close()
  }

  private def executeBatch(table: Table, mutationList: java.util.ArrayList[Mutation]): Unit = {
    logDebug(s"Writing ${mutationList.size()} mutations")
    table.batch(mutationList, null) //scalastyle:ignore
    mutationList.clear()
  }

  private def createMutation(row: InternalRow, fieldIndexes: Map[String, Int]): Mutation = {
    val operation = getOperation(row, fieldIndexes)
    val rowKey = row.getBinary(fieldIndexes(RowkeyAttribute))
    val columnFamily = row.getBinary(fieldIndexes(ColumnFamilyAttribute))
    val values: Option[Map[Array[Byte], Array[Byte]]] = fieldIndexes.get(ValuesAttribute)
      .map { idx =>
        val valueMap = row.getMap(idx)
        val keys = valueMap.keyArray().toSeq[Array[Byte]](BinaryType)
        val values = valueMap.valueArray().toSeq[Array[Byte]](BinaryType)

        keys.zip(values).toMap
      }

    def validateAndAddCell(values: Option[Map[Array[Byte], Array[Byte]]], mutation: Mutation): Mutation = {
      values.getOrElse {
        logError(s"$operation require at least one value defined in field $ValuesAttribute")
        throw new IllegalArgumentException(s"$operation: No value defined in field $ValuesAttribute")
      }.foreach { case (k, v) =>
        mutation match {
          case d: Delete => d.addColumn(columnFamily, k)
          case p: Put => p.addColumn(columnFamily, k, v)
        }
      }
      mutation
    }

    operation match {
      case DeleteRowOperation => new Delete(rowKey)
      case DeleteCellOperation => validateAndAddCell(values, new Delete(rowKey))
      case UpsertOperation => validateAndAddCell(values, new Put(rowKey))
    }
  }

  private def getOperation(row: InternalRow, fieldIdx: Map[String, Int]): String = {
    val operation = row.getString(fieldIdx(OperationAttribute))
    operation match {
      case DeleteRowOperation | DeleteCellOperation | UpsertOperation => operation
      case _ =>
        logError(
          s"Operation $operation is not valid. Valid operations are: $DeleteRowOperation, $DeleteCellOperation, " +
            s"$UpsertOperation}"
        )
        throw new IllegalArgumentException(s"Operation $operation is not valid")
    }
  }


}
