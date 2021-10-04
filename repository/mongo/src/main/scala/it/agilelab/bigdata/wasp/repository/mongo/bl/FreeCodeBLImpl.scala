package it.agilelab.bigdata.wasp.repository.mongo.bl

import it.agilelab.bigdata.wasp.repository.core.bl.FreeCodeBL
import it.agilelab.bigdata.wasp.models.FreeCodeModel
import it.agilelab.bigdata.wasp.repository.core.dbModels.FreeCodeDBModel
import it.agilelab.bigdata.wasp.repository.mongo.WaspMongoDB
import org.mongodb.scala.bson.{BsonDocument, BsonString}
import it.agilelab.bigdata.wasp.repository.core.mappers.FreeCodeMapperSelector.applyMap
import it.agilelab.bigdata.wasp.repository.core.mappers.FreeCodeMapperV1.fromModelToDBModel

class FreeCodeBLImpl(waspDB: WaspMongoDB) extends FreeCodeBL {

  def getByName(name: String): Option[FreeCodeModel] = {
    waspDB.getDocumentByField[FreeCodeDBModel]("name", new BsonString(name))
      .map(applyMap)
  }

  override def deleteByName(name: String): Unit =
    waspDB.deleteByName[FreeCodeDBModel](name)

  override def insert(freeCodeModel: FreeCodeModel): Unit =
    waspDB.insert[FreeCodeDBModel](fromModelToDBModel(freeCodeModel))

  override def upsert(freeCodeModel: FreeCodeModel): Unit =
    waspDB.upsert[FreeCodeDBModel](fromModelToDBModel(freeCodeModel))

  override def getAll: Seq[FreeCodeModel] =
    waspDB.getAll[FreeCodeDBModel]().map(applyMap)



}
