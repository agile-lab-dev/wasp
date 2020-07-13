package it.agile.bigdata.wasp.repository.postgres.utils

trait ConnectionInfoProvider {
  protected def getUrl: String
  protected def getUser: String
  protected def getPassword: String
  protected def getDriver: String
  protected def getPoolSize : Int
}
