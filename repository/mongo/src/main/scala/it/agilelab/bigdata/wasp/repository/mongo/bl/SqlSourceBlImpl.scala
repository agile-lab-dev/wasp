package it.agilelab.bigdata.wasp.repository.mongo.bl

import it.agilelab.bigdata.wasp.repository.core.bl.SqlSourceBl
import it.agilelab.bigdata.wasp.models.SqlSourceModel
import it.agilelab.bigdata.wasp.repository.core.dbModels.{SqlSourceDBModel, SqlSourceDBModelV1}
import it.agilelab.bigdata.wasp.repository.core.mappers.SqlSourceMapperV1.transform
import it.agilelab.bigdata.wasp.repository.core.mappers.SqlSourceMapperSelector.factory
import it.agilelab.bigdata.wasp.repository.mongo.WaspMongoDB
import org.bson.BsonString

class SqlSourceBlImpl(waspDB: WaspMongoDB) extends SqlSourceBl {

  def getByName(name: String): Option[SqlSourceModel] = {
    waspDB.getDocumentByField[SqlSourceDBModel]("name", new BsonString(name)).map(factory)
  }

  override def persist(rawModel: SqlSourceModel): Unit =
    waspDB.insert[SqlSourceDBModel](transform[SqlSourceDBModelV1](rawModel))

  override def upsert(rawModel: SqlSourceModel): Unit =
    waspDB.upsert[SqlSourceDBModel](transform[SqlSourceDBModelV1](rawModel))
}
