package it.agilelab.bigdata.wasp.repository.postgres.utils

import java.sql.{Connection, Driver, DriverManager}

import org.apache.commons.dbcp2.{DriverManagerConnectionFactory, PoolableConnectionFactory, PoolingDriver}
import org.apache.commons.pool2.impl.GenericObjectPool
import ConnectionSupport.poolingDriver

trait ConnectionSupport {
  this: ConnectionInfoProvider =>

  private val poolName= s"$getUrl:$getUser"

  protected def getConnection() : Connection = {

    if (!poolingDriver.getPoolNames.toSet.contains(poolName)) {
      ConnectionSupport synchronized {
        if (!poolingDriver.getPoolNames.toSet.contains(poolName)) {
          createAndRegisterPool(getUrl, getUser, getPassword, getDriver, poolName, poolingDriver)
        }
      }
    }
    DriverManager.getConnection("jdbc:apache:commons:dbcp:" + poolName)
  }

  private def createAndRegisterPool(url: String,
                                    user: String,
                                    password: String,
                                    driver: String,
                                    poolName: String,
                                    poolingDriver: PoolingDriver): Unit = {
    registerDriver(driver)
    val driverManagerConnectionFactory = new DriverManagerConnectionFactory(url, user, password)
    val poolableConnectionFactory = new PoolableConnectionFactory(driverManagerConnectionFactory, null)
    val connectionPool = new GenericObjectPool(poolableConnectionFactory)
    connectionPool.setMaxIdle(getPoolSize)
    connectionPool.setMaxTotal(getPoolSize)

    poolableConnectionFactory.setPool(connectionPool)
    poolingDriver.registerPool(poolName, connectionPool)
  }



  private def registerDriver(driverClassName: String): Unit = {
    val driverClass = this.getClass.getClassLoader.loadClass(driverClassName)
    val driverInstance = driverClass.getDeclaredConstructor().newInstance().asInstanceOf[Driver]
    DriverManager.registerDriver(driverInstance)
  }



  protected def closePool(): Unit = poolingDriver.closePool(poolName)

}

object ConnectionSupport {
  private[utils] val poolingDriver = new PoolingDriver()
}