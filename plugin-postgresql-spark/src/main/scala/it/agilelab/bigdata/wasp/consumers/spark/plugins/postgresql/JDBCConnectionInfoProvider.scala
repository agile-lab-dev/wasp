package it.agilelab.bigdata.wasp.consumers.spark.plugins.postgresql

import java.util.Properties

trait JDBCConnectionInfoProvider {
  def getUrl: String
  def getDriver: String
  def getProperties: Properties
}
