package it.agilelab.bigdata.wasp.repository.mongo.bl

import it.agilelab.bigdata.wasp.models.GenericModel
import it.agilelab.bigdata.wasp.repository.core.bl.GenericBL
import it.agilelab.bigdata.wasp.repository.core.dbModels.{GenericDBModel, GenericDBModelV1}
import it.agilelab.bigdata.wasp.repository.mongo.WaspMongoDB
import org.bson.BsonString
import it.agilelab.bigdata.wasp.repository.core.mappers.GenericMapperSelector.factory
import it.agilelab.bigdata.wasp.repository.core.mappers.GenericMapperV1.transform

class GenericBLImp(waspDB: WaspMongoDB) extends GenericBL  {

  def getByName(name: String) = {
    waspDB.getDocumentByField[GenericDBModel]("name", new BsonString(name)).map(factory)
  }

  override def getAll(): Seq[GenericModel] = waspDB.getAll[GenericDBModel].map(factory)

  override def persist(genericModel: GenericModel): Unit =
    waspDB.insert[GenericDBModel](transform[GenericDBModelV1](genericModel))

  override def upsert(genericModel: GenericModel): Unit =
    waspDB.upsert[GenericDBModel](transform[GenericDBModelV1](genericModel))
}
