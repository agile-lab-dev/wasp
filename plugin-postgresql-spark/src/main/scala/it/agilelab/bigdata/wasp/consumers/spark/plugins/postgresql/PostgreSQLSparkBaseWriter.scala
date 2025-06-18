package it.agilelab.bigdata.wasp.consumers.spark.plugins.postgresql

import it.agilelab.bigdata.wasp.models.SQLSinkModel

import java.sql.Connection
import java.util.Properties

/** Base class for writers that write to PostgreSQL using upserts (INSERT ON CONFLICT)
  */
trait PostgreSQLSparkBaseWriter extends JDBCPooledConnectionSupport with JDBCConnectionInfoProvider {

  def sqlSinkModel: SQLSinkModel

  protected def getMetadataFetcherService(): JDBCMetadataFetcherService = new JDBCMetadataFetcherServiceImpl()

  override def getPoolSize: Int = sqlSinkModel.poolSize

  override def getUrl: String = sqlSinkModel.jdbcConnection.url

  override def getDriver: String = sqlSinkModel.jdbcConnection.driverName

  override def getProperties: Properties = {
    val props = new Properties()

    props.put("user", sqlSinkModel.jdbcConnection.user)
    props.put("password", sqlSinkModel.jdbcConnection.password)

    sqlSinkModel.jdbcConnection.properties.getOrElse(Map.empty).foreach(entry => props.put(entry._1, entry._2))

    props
  }

  protected def createConnection(): Connection = {
    val connection = createConnectionFactory() // TODO maybe this should not be a factory?
    connection.setAutoCommit(false)
    connection
  }

  protected def fetchMetadata(table: String): TableMetadata = {
    try {
      val connection = createConnection()
      try {
        getMetadataFetcherService().fetchMetadataForTable(connection, table)
      } finally {
        connection.close()
      }
    } catch {
      case e: Exception =>
        throw e
    }
  }

}
