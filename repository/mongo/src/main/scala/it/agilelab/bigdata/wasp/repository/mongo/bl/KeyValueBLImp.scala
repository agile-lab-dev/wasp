package it.agilelab.bigdata.wasp.repository.mongo.bl

import it.agilelab.bigdata.wasp.repository.core.bl.KeyValueBL
import it.agilelab.bigdata.wasp.models.KeyValueModel
import it.agilelab.bigdata.wasp.repository.core.dbModels.{KeyValueDBModel, KeyValueDBModelV1}
import it.agilelab.bigdata.wasp.repository.mongo.WaspMongoDB
import org.bson.BsonString
import it.agilelab.bigdata.wasp.repository.core.mappers.KeyValueMapperSelector.factory
import it.agilelab.bigdata.wasp.repository.core.mappers.KeyValueMapperV1.transform

class KeyValueBLImp(waspDB: WaspMongoDB) extends KeyValueBL {

  def getByName(name: String): Option[KeyValueModel] = {
    waspDB
      .getDocumentByField[KeyValueDBModel]("name", new BsonString(name))
      .map(factory)
  }

  override def persist(model: KeyValueModel): Unit = waspDB.insert[KeyValueDBModel](transform[KeyValueDBModelV1](model))

  override def upsert(model: KeyValueModel): Unit = waspDB.upsert[KeyValueDBModel](transform[KeyValueDBModelV1](model))

  override def getAll(): Seq[KeyValueModel] = waspDB.getAll[KeyValueDBModel].map(factory)
}
