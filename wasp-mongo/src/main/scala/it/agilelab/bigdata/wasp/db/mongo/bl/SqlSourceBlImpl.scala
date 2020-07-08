package it.agilelab.bigdata.wasp.db.mongo.bl

import it.agilelab.bigdata.wasp.core.bl.SqlSourceBl
import it.agilelab.bigdata.wasp.core.models.SqlSourceModel
import it.agilelab.bigdata.wasp.db.mongo.WaspMongoDB
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
    waspDB.getDocumentByField[SqlSourceModel]("name", new BsonString(name)).map(index => {
      factory(index)
    })
  }


  override def persist(rawModel: SqlSourceModel): Unit = waspDB.insert[SqlSourceModel](rawModel)

  override def upsert(rawModel: SqlSourceModel): Unit = waspDB.upsert[SqlSourceModel](rawModel)
}
