package it.agilelab.bigdata.wasp.db.mongo

import it.agilelab.bigdata.wasp.core.bl.FactoryBL
import it.agilelab.bigdata.wasp.core.db.{WaspDB, WaspDBService}
import it.agilelab.bigdata.wasp.db.mongo.bl.MongoFactoryBL

class WaspMongoDBService extends WaspDBService {

  override def getDB(): WaspDB = WaspMongoDB.getDB()

  override def initializeDB(): WaspDB = WaspMongoDB.initializeDB()

  override def dropDatabase(): Unit = WaspMongoDB.dropDatabase()

  override def getFactoryBL: FactoryBL = new MongoFactoryBL

}
