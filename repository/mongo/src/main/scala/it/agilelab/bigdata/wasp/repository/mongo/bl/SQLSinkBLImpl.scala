package it.agilelab.bigdata.wasp.repository.mongo.bl

import it.agilelab.bigdata.wasp.models.SQLSinkModel
import it.agilelab.bigdata.wasp.repository.core.bl.SQLSinkBL
import it.agilelab.bigdata.wasp.repository.mongo.WaspMongoDB
import org.bson.BsonString

class SQLSinkBLImpl(waspDB: WaspMongoDB) extends SQLSinkBL {

  def getByName(name: String): Option[SQLSinkModel] = {
    waspDB.getDocumentByField[SQLSinkModel]("name", new BsonString(name))
  }

  override def persist(model: SQLSinkModel): Unit = waspDB.insert[SQLSinkModel](model)

  override def upsert(model: SQLSinkModel): Unit = waspDB.upsert[SQLSinkModel](model)

  override def getAll(): Seq[SQLSinkModel] = waspDB.getAll[SQLSinkModel]

  override def deleteByName(name: String): Unit = waspDB.deleteByName[SQLSinkModel](name)

}
