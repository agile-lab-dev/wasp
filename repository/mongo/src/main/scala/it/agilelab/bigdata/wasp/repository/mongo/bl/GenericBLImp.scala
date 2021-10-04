package it.agilelab.bigdata.wasp.repository.mongo.bl

import it.agilelab.bigdata.wasp.models.GenericModel
import it.agilelab.bigdata.wasp.repository.core.bl.GenericBL
import it.agilelab.bigdata.wasp.repository.core.dbModels.GenericDBModel
import it.agilelab.bigdata.wasp.repository.mongo.WaspMongoDB
import org.bson.BsonString
import it.agilelab.bigdata.wasp.repository.core.mappers.GenericMapperSelector.applyMap
import it.agilelab.bigdata.wasp.repository.core.mappers.GenericMapperV1.fromModelToDBModel

class GenericBLImp(waspDB: WaspMongoDB) extends GenericBL  {

  def getByName(name: String) = {
    waspDB.getDocumentByField[GenericDBModel]("name", new BsonString(name)).map(applyMap)
  }

  override def getAll(): Seq[GenericModel] = waspDB.getAll[GenericDBModel].map(applyMap)

  override def persist(genericModel: GenericModel): Unit =
    waspDB.insert[GenericDBModel](fromModelToDBModel(genericModel))

  override def upsert(genericModel: GenericModel): Unit =
    waspDB.upsert[GenericDBModel](fromModelToDBModel(genericModel))
}
