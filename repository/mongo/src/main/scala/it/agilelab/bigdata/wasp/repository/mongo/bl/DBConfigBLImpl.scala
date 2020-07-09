package it.agilelab.bigdata.wasp.repository.mongo.bl

import it.agilelab.bigdata.wasp.repository.core.bl.DBConfigBL
import it.agilelab.bigdata.wasp.repository.mongo.utils.MongoDBHelper._
import it.agilelab.bigdata.wasp.repository.mongo.WaspMongoDB

class DBConfigBLImpl(waspDB: WaspMongoDB) extends DBConfigBL {

  def retrieveDBConfig(): Seq[String] = {
    waspDB.mongoDatabase.getCollection(WaspMongoDB.configurationsName)
      .find().results().map(_.toJson())

  }
}
