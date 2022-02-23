package it.agilelab.bigdata.wasp.consumers.spark.plugins.plain.hbase.integration

import it.agilelab.bigdata.wasp.consumers.spark.plugins.plain.hbase.integration.sink.HBaseWriterProperties
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory


case class HBaseTableCatalog(namespace: Option[String], tableName: String, columnFamilies: List[String]) {

  val fullTableName: String = namespace.map(n => s"$n:$tableName").getOrElse(tableName)

}

object HBaseTableCatalog {

  private val logger = LoggerFactory.getLogger(getClass)

  // If defined and larger than 3, a new table will be created with the number of region specified.
  val newTable = "newtable"
  // The json string specifying hbase catalog information
  val regionStart = "regionStart"
  val defaultRegionStart = "aaaaaaa"
  val regionEnd = "regionEnd"
  val defaultRegionEnd = "zzzzzzz"
  // The namespace of hbase table
  val namespace = "namespace"
  // The name of hbase table
  val tableName = "tableName"
  val columnFamilies = "columnFamilies"

  val schema: StructType = StructType(
    Array(
      StructField(HBaseWriterProperties.OperationAttribute, StringType, nullable = false),
      StructField(HBaseWriterProperties.RowkeyAttribute, BinaryType, nullable = false),
      StructField(HBaseWriterProperties.ColumnFamilyAttribute, BinaryType, nullable = false),
      StructField(HBaseWriterProperties.ValuesAttribute, MapType(BinaryType, BinaryType), nullable = false)
    )
  )

  def apply(params: Map[String, String]): HBaseTableCatalog = {
    val namespace = params.get(HBaseTableCatalog.namespace)
    val tableName = params.get(HBaseTableCatalog.tableName)
      .filter(_.nonEmpty)
      .getOrElse {
        logger.error("Unable to create Hbase Table Catalog, hbase table name option is mandatory!")
        throw new IllegalArgumentException("Table name option mandatory")
      }
    val columnFamilies = params.get(HBaseTableCatalog.columnFamilies).map(_.split(',').toList).getOrElse(List.empty)
    HBaseTableCatalog(namespace, tableName, columnFamilies)
  }
}