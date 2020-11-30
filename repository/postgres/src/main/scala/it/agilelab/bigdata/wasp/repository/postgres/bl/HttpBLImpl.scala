package it.agilelab.bigdata.wasp.repository.postgres.bl

import it.agilelab.bigdata.wasp.models.HttpModel
import it.agilelab.bigdata.wasp.repository.core.bl.HttpBL
import it.agilelab.bigdata.wasp.repository.postgres.WaspPostgresDB
import it.agilelab.bigdata.wasp.repository.postgres.tables.{HttpTableDefinition, TableDefinition}

case class HttpBLImpl(waspDB: WaspPostgresDB) extends HttpBL with PostgresBL {
  override implicit val tableDefinition: TableDefinition[HttpModel, String] = HttpTableDefinition
  override def getByName(name: String): Option[HttpModel] = waspDB.getByPrimaryKey(name)

  override def persist(model: HttpModel): Unit = waspDB.insert(model)

  override def getAll(): Seq[HttpModel] = waspDB.getAll()

  override def upsert(model: HttpModel): Unit = waspDB.upsert(model)

  override def insertIfNotExists(model: HttpModel): Unit = waspDB.insertIfNotExists(model)

}
