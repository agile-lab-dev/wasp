package it.agilelab.bigdata.wasp.consumers.spark.plugins.postgresql

import it.agilelab.bigdata.wasp.core.logging.Logging

import java.sql.Connection

/**
  * Service for fetching metadata for a table using JDBC.
  *
  * No attempt at caching metadata is made, as such frequent invocations are to be avoided and the metadata should be
  * cached/reused by the caller.
  *
  * @author Nicolò Bidotti
  */
trait JDBCMetadataFetcherService extends Logging {

  def fetchMetadataForTable(connection: Connection, tableName: String): TableMetadata
}
