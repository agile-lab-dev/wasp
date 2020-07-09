package it.agilelab.bigdata.wasp.repository.mongo

import it.agilelab.bigdata.wasp.repository.core.bl.FactoryBL
import it.agilelab.bigdata.wasp.repository.core.db.{RepositoriesFactory, WaspDB}
import it.agilelab.bigdata.wasp.repository.mongo.bl.MongoFactoryBL

class MongoRepositoriesFactory extends RepositoriesFactory {

  override def getDB(): WaspDB = WaspMongoDB.getDB()

  override def initializeDB(): WaspDB = WaspMongoDB.initializeDB()

  override def dropDatabase(): Unit = WaspMongoDB.dropDatabase()

  override def getFactoryBL: FactoryBL = new MongoFactoryBL

}
