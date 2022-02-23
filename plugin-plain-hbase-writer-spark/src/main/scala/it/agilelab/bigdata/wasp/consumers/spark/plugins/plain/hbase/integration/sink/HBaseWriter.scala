package it.agilelab.bigdata.wasp.consumers.spark.plugins.plain.hbase.integration.sink

import it.agilelab.bigdata.wasp.consumers.spark.plugins.plain.hbase.integration.{HBaseConnectionCache, HBaseContext, HBaseCredentialsManager, HBaseTableCatalog}
import org.apache.hadoop.hbase.TableName
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.types._

object HBaseWriter extends Logging with Serializable {

  import HBaseWriterProperties._

  def write(queryExecution: QueryExecution,
            params: Map[String, String],
            hBaseContext: HBaseContext,
            schema: StructType): Unit = {

    validateQuery(queryExecution.analyzed.output)

    val batchSize = params.get("batchSize").map(_.toInt).getOrElse(1000)

    val table = HBaseTableCatalog(params).fullTableName
    val fieldIdx = fieldIndexes(schema)

    queryExecution
      .toRdd
      .foreachPartition { iter: Iterator[InternalRow] =>
        val config = hBaseContext.getConf()
        HBaseCredentialsManager.applyCredentials()
        val smartConnection = HBaseConnectionCache.getConnection(config)
        val tableName = TableName.valueOf(table)
        try {
          HBaseWriterTask.mutate(iter, tableName, smartConnection.connection, fieldIdx, batchSize)
        } catch {
          case e: Throwable => logError("Unable to write hbase mutation, reason:", e)
        } finally {
          smartConnection.close()
        }
      }
  }

  private def fieldIndexes(schema: StructType): Map[String, Int] = {
    AttributeTypes.keys.map(attributeName => attributeName -> schema.fieldIndex(attributeName)).toMap
  }

  def validateQuery(schema: Seq[Attribute]): Unit = {
    AttributeTypes.foreach { case (k, (v, required)) => validateSchema(schema, k, v, required) }
  }

  private def validateSchema(schema: Seq[Attribute], attrName: String, attrType: DataType, required: Boolean): Unit = {
    schema.find(_.name == attrName) match {
      case Some(expr) =>
        if (!validateType(expr.dataType, attrType)) {
          log.error("{} attribute type {} not supported. It must be {}",
            attrName,
            expr.dataType.catalogString,
            attrType)
          throw new IllegalStateException(
            s"$attrName attribute unsupported type ${expr.dataType.catalogString}. It must be a $attrType"
          )
        }
      case None => if (required) throw new IllegalStateException(s"$attrName is mandatory")
    }
  }

  private def validateType(expected: DataType, actual: DataType): Boolean = {
    (actual, expected) match {
      case (MapType(aKeyType, aValueType, _), MapType(eKeyType, eValueType, _)) =>
        aKeyType.typeName == eKeyType.typeName && aValueType.typeName == eValueType.typeName
      case _ => expected.typeName == actual.typeName
    }
  }

}

object HBaseWriterProperties extends Serializable {
  val OperationAttribute = "operation"
  val RowkeyAttribute = "rowKey"
  val ColumnFamilyAttribute = "columnFamily"
  val ValuesAttribute = "values"

  val AttributeTypes = Map((OperationAttribute, (StringType, true)),
    (RowkeyAttribute, (BinaryType, true)),
    (ColumnFamilyAttribute, (BinaryType, true)),
    (ValuesAttribute, (MapType(BinaryType, BinaryType), false))
  )

  val UpsertOperation = "upsert"
  val DeleteRowOperation = "delete-row"
  val DeleteCellOperation = "delete-cell"
}
