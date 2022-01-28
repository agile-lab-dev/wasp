package it.agilelab.bigdata.wasp.repository.mongo.bl

import it.agilelab.bigdata.wasp.repository.core.bl.RawBL
import it.agilelab.bigdata.wasp.models.RawModel
import it.agilelab.bigdata.wasp.repository.core.dbModels.{RawDBModel, RawDBModelV1}
import it.agilelab.bigdata.wasp.repository.mongo.WaspMongoDB
import org.bson.BsonString
import it.agilelab.bigdata.wasp.repository.core.mappers.RawMapperSelector.factory
import it.agilelab.bigdata.wasp.repository.core.mappers.RawMapperV1.transform

class RawBLImp(waspDB: WaspMongoDB) extends RawBL  {

  def getByName(name: String) = {
    waspDB.getDocumentByField[RawDBModel]("name", new BsonString(name)).map(factory)
  }

  override def getAll(): Seq[RawModel] =
    waspDB.getAll[RawDBModel].map(factory)

  override def persist(rawModel: RawModel): Unit =
    waspDB.insert[RawDBModel](transform[RawDBModelV1](rawModel))

  override def upsert(rawModel: RawModel): Unit =
    waspDB.upsert[RawDBModel](transform[RawDBModelV1](rawModel))
}
