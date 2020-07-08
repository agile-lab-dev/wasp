package it.agilelab.bigdata.wasp.db.mongo.bl

import it.agilelab.bigdata.wasp.core.bl.ProcessGroupBL
import it.agilelab.bigdata.wasp.core.models.ProcessGroupModel
import it.agilelab.bigdata.wasp.db.mongo.WaspMongoDB
import org.mongodb.scala.bson.BsonString

class ProcessGroupBLImpl(waspDB: WaspMongoDB) extends ProcessGroupBL {

  override def getById(pgId: String): Option[ProcessGroupModel] =
    waspDB
      .getAllDocumentsByField[ProcessGroupModel]("name", BsonString(pgId))
      .headOption

  override def insert(processGroup: ProcessGroupModel): Unit =
    waspDB.insert[ProcessGroupModel](processGroup)

}
