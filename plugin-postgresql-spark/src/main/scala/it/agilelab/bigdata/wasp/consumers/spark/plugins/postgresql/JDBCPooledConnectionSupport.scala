package it.agilelab.bigdata.wasp.consumers.spark.plugins.postgresql

import java.sql.{Connection, Driver, DriverManager}
import org.apache.commons.dbcp2.{DriverManagerConnectionFactory, PoolableConnectionFactory, PoolingDriver}
import org.apache.commons.pool2.impl.GenericObjectPool

import java.util.Properties

/** Provides support for pooled JDBC connections using DBCP.
  *
  * @author
  *   Nicolò Bidotti
  */
trait JDBCPooledConnectionSupport {
  this: JDBCConnectionInfoProvider =>

  import JDBCPooledConnectionSupport.poolingDriver

  def getPoolSize: Int

  def createConnectionFactory: () => Connection = {
    val poolName = createPoolName(getUrl, getUser)

    if (!poolingDriver.getPoolNames.toSet.contains(poolName)) {
      JDBCPooledConnectionSupport synchronized {
        if (!poolingDriver.getPoolNames.toSet.contains(poolName)) {
          createAndRegisterPool(getUrl, getProperties, getDriver, poolName, poolingDriver)
        }
      }
    }

    () => DriverManager.getConnection("jdbc:apache:commons:dbcp:" + poolName)
  }

  private def createAndRegisterPool(
      url: String,
      properties: Properties,
      driver: String,
      poolName: String,
      poolingDriver: PoolingDriver
  ): Unit = {
    registerDriver(driver)
    val connectionFactory         = new DriverManagerConnectionFactory(url, properties)
    val poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory, null)
    // TODO set default auto commit, validation query, timeouts
    val connectionPool = new GenericObjectPool(poolableConnectionFactory)
    connectionPool.setMaxIdle(getPoolSize)
    connectionPool.setMaxTotal(getPoolSize)

    poolableConnectionFactory.setPool(connectionPool)
    poolingDriver.registerPool(poolName, connectionPool)
  }

  private def getUser: String = getProperties.getProperty("user")

  // Used for tests
  protected def getConnectionActiveSize: Int = {
    poolingDriver.getConnectionPool(createPoolName(getUrl, getUser)).getNumActive
  }

  private def registerDriver(driverClassName: String): Unit = {
    val driverClass    = this.getClass.getClassLoader.loadClass(driverClassName)
    val driverInstance = driverClass.getConstructor().newInstance().asInstanceOf[Driver]
    DriverManager.registerDriver(driverInstance)
  }

  private def createPoolName(url: String, user: String) = s"$url:$user"

  protected def closePool(): Unit = poolingDriver.closePool(createPoolName(getUrl, getUser))

}

object JDBCPooledConnectionSupport {
  val poolingDriver =
    new PoolingDriver() // PoolingDriver does not have static accessor for pool names, so we need an instance
}
