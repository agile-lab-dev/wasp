package it.agilelab.bigdata.wasp.core.bl

import it.agilelab.bigdata.wasp.core.models.SqlSourceModel
import it.agilelab.bigdata.wasp.core.utils.WaspDB
import org.bson.BsonString

trait SqlSourceBl {

  def getByName(name: String): Option[SqlSourceModel]

  def persist(rawModel: SqlSourceModel): Unit
}

class SqlSourceBlImpl(waspDB: WaspDB) extends SqlSourceBl {

  private def factory(s: SqlSourceModel) = SqlSourceModel(
    s.name,
    s.connectionName,
    s.database,
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
}