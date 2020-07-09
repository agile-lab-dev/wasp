package it.agilelab.bigdata.wasp.repository.core.db

import java.util.ServiceLoader

import it.agilelab.bigdata.wasp.repository.core.bl.FactoryBL

import scala.collection.JavaConverters._

trait RepositoriesFactory {
  def getDB() : WaspDB
  def initializeDB() : WaspDB
  def dropDatabase() : Unit
  def getFactoryBL : FactoryBL

}

object RepositoriesFactory {
  lazy val service = {
    val dbLoaders :  ServiceLoader[RepositoriesFactory] = ServiceLoader.load[RepositoriesFactory](classOf[RepositoriesFactory])
    val dbServiceList = dbLoaders.iterator().asScala.toList
    require(dbServiceList.size==1,s"The trait WaspDBHelper must have only one implementation : ${dbServiceList.mkString(",")}")
    dbServiceList.head
  }
}
