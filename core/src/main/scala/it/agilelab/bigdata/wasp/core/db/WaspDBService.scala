package it.agilelab.bigdata.wasp.core.db

import java.util.ServiceLoader

import it.agilelab.bigdata.wasp.core.bl.FactoryBL

import scala.collection.JavaConverters._

trait WaspDBService {
  def getDB() : WaspDB
  def initializeDB() : WaspDB
  def dropDatabase() : Unit
  def getFactoryBL : FactoryBL

}

object WaspDBService {
  lazy val service = {
    val dbLoaders :  ServiceLoader[WaspDBService] = ServiceLoader.load[WaspDBService](classOf[WaspDBService])
    val dbServiceList = dbLoaders.iterator().asScala.toList
    require(dbServiceList.size==1,s"The trait WaspDBHelper must have only one implementation : ${dbServiceList.mkString(",")}")
    dbServiceList.head
  }
}
