package it.agilelab.bigdata.wasp.repository.mongo.bl

import it.agilelab.bigdata.wasp.models.FreeCodeModel
import it.agilelab.bigdata.wasp.repository.core.bl.FreeCodeBL
import it.agilelab.bigdata.wasp.repository.core.dbModels.{FreeCodeDBModel, FreeCodeDBModelV1}
import it.agilelab.bigdata.wasp.repository.core.mappers.FreeCodeMapperSelector.factory
import it.agilelab.bigdata.wasp.repository.core.mappers.FreeCodeMapperV1.transform
import it.agilelab.bigdata.wasp.repository.mongo.WaspMongoDB
import org.mongodb.scala.bson.BsonString

class FreeCodeBLImpl(waspDB: WaspMongoDB) extends FreeCodeBL {

  def getByName(name: String): Option[FreeCodeModel] = {
    waspDB.getDocumentByField[FreeCodeDBModel]("name", new BsonString(name))
      .map(factory)
  }

  override def deleteByName(name: String): Unit =
    waspDB.deleteByName[FreeCodeDBModel](name)

  override def insert(freeCodeModel: FreeCodeModel): Unit =
    waspDB.insert[FreeCodeDBModel](transform[FreeCodeDBModelV1](freeCodeModel))

  override def upsert(freeCodeModel: FreeCodeModel): Unit =
    waspDB.upsert[FreeCodeDBModel](transform[FreeCodeDBModelV1](freeCodeModel))

  override def getAll: Seq[FreeCodeModel] =
    waspDB.getAll[FreeCodeDBModel]().map(factory)



}
