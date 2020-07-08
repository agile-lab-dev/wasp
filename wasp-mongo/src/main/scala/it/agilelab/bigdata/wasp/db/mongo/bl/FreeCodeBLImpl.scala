package it.agilelab.bigdata.wasp.db.mongo.bl

import it.agilelab.bigdata.wasp.core.bl.FreeCodeBL
import it.agilelab.bigdata.wasp.core.models.FreeCodeModel
import it.agilelab.bigdata.wasp.db.mongo.WaspMongoDB
import org.mongodb.scala.bson.{BsonDocument, BsonString}

class FreeCodeBLImpl(waspDB: WaspMongoDB) extends FreeCodeBL {

  private def factory(t: BsonDocument): FreeCodeModel = {
    FreeCodeModel(t.get("name").asString().getValue,t.get("code").asString().getValue)
  }

  def getByName(name: String): Option[FreeCodeModel] = {
    waspDB.getDocumentByFieldRaw[FreeCodeModel]("name", new BsonString(name))
      .map(factory)
  }

  override def deleteByName(name: String): Unit = waspDB.deleteByName[FreeCodeModel](name)

  override def insert(freeCodeModel: FreeCodeModel): Unit = waspDB.insert[FreeCodeModel](freeCodeModel)

  override def getAll: Seq[FreeCodeModel] = waspDB.getAllRaw[FreeCodeModel]().map(factory)



}
