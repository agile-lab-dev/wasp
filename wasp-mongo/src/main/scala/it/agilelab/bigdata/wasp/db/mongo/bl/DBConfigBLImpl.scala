package it.agilelab.bigdata.wasp.db.mongo.bl

import it.agilelab.bigdata.wasp.core.bl.DBConfigBL
import it.agilelab.bigdata.wasp.db.mongo.WaspMongoDB
import it.agilelab.bigdata.wasp.db.mongo.utils.MongoDBHelper._

class DBConfigBLImpl(waspDB: WaspMongoDB) extends DBConfigBL {

  def retrieveDBConfig(): Seq[String] = {
    waspDB.mongoDatabase.getCollection(WaspMongoDB.configurationsName)
      .find().results().map(_.toJson())

  }
}
