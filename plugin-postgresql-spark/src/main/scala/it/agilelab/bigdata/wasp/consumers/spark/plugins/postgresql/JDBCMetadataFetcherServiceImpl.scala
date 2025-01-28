package it.agilelab.bigdata.wasp.consumers.spark.plugins.postgresql

import java.sql.{Connection, ResultSetMetaData}

/**
  * Service for fetching metadata for a table using JDBC.
  *
  * No attempt at caching metadata is made, as such frequent invocations are to be avoided and the metadata should be
  * cached/reused by the caller.
  *
  * @author Nicolò Bidotti
  */
class JDBCMetadataFetcherServiceImpl extends JDBCMetadataFetcherService {
  override def fetchMetadataForTable(connection: Connection, tableName: String): TableMetadata = {
    logger.info(s"Fetching metadata for table $tableName")
    val statement = connection.prepareStatement(s"select * from $tableName where 1=0")
    try {
      val resultSet = statement.executeQuery()
      try {
        val resultSetMetaData = resultSet.getMetaData
        val numColumns        = resultSetMetaData.getColumnCount
        val columnsMetadata   = new Array[ColumnMetadata](numColumns)
        var i                 = 0
        while (i < numColumns) {
          val columnName = resultSetMetaData.getColumnLabel(i + 1)
          val typeNumber = resultSetMetaData.getColumnType(i + 1)
          val typeName   = resultSetMetaData.getColumnTypeName(i + 1)
          // Not Used
          //          val fieldSize = resultSetMetaData.getPrecision(i + 1)
          //          val fieldScale = resultSetMetaData.getScale(i + 1)
          //          val isSigned = resultSetMetaData.isSigned(i + 1)
          val nullable = resultSetMetaData.isNullable(i + 1) != ResultSetMetaData.columnNoNulls
          columnsMetadata(i) = ColumnMetadata(columnName, i, typeNumber, typeName, nullable)
          i += 1
        }
        new TableMetadata(tableName, columnsMetadata.toList)
      } finally {
        resultSet.close()
      }
    } finally {
      statement.close()
    }
  }
}
