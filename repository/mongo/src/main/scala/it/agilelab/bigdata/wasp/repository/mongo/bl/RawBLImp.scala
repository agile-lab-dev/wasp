package it.agilelab.bigdata.wasp.repository.mongo.bl

import it.agilelab.bigdata.wasp.repository.core.bl.RawBL
import it.agilelab.bigdata.wasp.models.RawModel
import it.agilelab.bigdata.wasp.repository.core.dbModels.RawDBModel
import it.agilelab.bigdata.wasp.repository.mongo.WaspMongoDB
import org.bson.BsonString
import it.agilelab.bigdata.wasp.repository.core.mappers.RawMapperSelector.applyMap
import it.agilelab.bigdata.wasp.repository.core.mappers.RawMapperV1.fromModelToDBModel

class RawBLImp(waspDB: WaspMongoDB) extends RawBL  {

  def getByName(name: String) = {
    waspDB.getDocumentByField[RawDBModel]("name", new BsonString(name)).map(applyMap)
  }

  override def getAll(): Seq[RawModel] =
    waspDB.getAll[RawDBModel].map(applyMap)

  override def persist(rawModel: RawModel): Unit =
    waspDB.insert[RawDBModel](fromModelToDBModel(rawModel))

  override def upsert(rawModel: RawModel): Unit =
    waspDB.upsert[RawDBModel](fromModelToDBModel(rawModel))
}
