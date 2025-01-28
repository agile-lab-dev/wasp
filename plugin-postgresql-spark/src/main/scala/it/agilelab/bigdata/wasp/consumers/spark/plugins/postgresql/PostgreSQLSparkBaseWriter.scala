package it.agilelab.bigdata.wasp.consumers.spark.plugins.postgresql

import it.agilelab.bigdata.wasp.models.SQLSinkModel

import java.sql.Connection
import java.util.Properties
import scala.collection.JavaConverters._

/**
	* Base class for writers that write to PostgreSQL using upserts (INSERT ON CONFLICT)
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
    // this can technically overwrite the user/password we set above, but the JDBCConnectionConfig enforces that they
    // are not present in the properties so it should be fine unless somebody really wants to mess with us
    sqlSinkModel.jdbcConnection.properties.foreach(propsMap => props.putAll(propsMap.asJava))

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
