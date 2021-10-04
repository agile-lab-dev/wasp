package it.agilelab.bigdata.wasp.repository.mongo.bl

import it.agilelab.bigdata.wasp.repository.core.bl.SqlSourceBl
import it.agilelab.bigdata.wasp.models.SqlSourceModel
import it.agilelab.bigdata.wasp.repository.core.dbModels.SqlSourceDBModel
import it.agilelab.bigdata.wasp.repository.core.mappers.SqlSourceMapperV1.fromModelToDBModel
import it.agilelab.bigdata.wasp.repository.core.mappers.SqlSourceMapperSelector.applyMap
import it.agilelab.bigdata.wasp.repository.mongo.WaspMongoDB
import org.bson.BsonString

class SqlSourceBlImpl(waspDB: WaspMongoDB) extends SqlSourceBl {

  private def factory(s: SqlSourceModel) = SqlSourceModel(
    s.name,
    s.connectionName,
    s.dbtable,
    s.partitioningInfo,
    s.numPartitions,
    s.fetchSize
  )

  def getByName(name: String): Option[SqlSourceModel] = {
    waspDB.getDocumentByField[SqlSourceDBModel]("name", new BsonString(name)).map(applyMap)
  }


  override def persist(rawModel: SqlSourceModel): Unit =
    waspDB.insert[SqlSourceDBModel](fromModelToDBModel(rawModel))

  override def upsert(rawModel: SqlSourceModel): Unit =
    waspDB.upsert[SqlSourceDBModel](fromModelToDBModel(rawModel))
}
