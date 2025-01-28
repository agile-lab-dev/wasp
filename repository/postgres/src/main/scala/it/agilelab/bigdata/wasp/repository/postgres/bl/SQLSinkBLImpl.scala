package it.agilelab.bigdata.wasp.repository.postgres.bl

import it.agilelab.bigdata.wasp.models.SQLSinkModel
import it.agilelab.bigdata.wasp.repository.core.bl.SQLSinkBL
import it.agilelab.bigdata.wasp.repository.postgres.WaspPostgresDB
import it.agilelab.bigdata.wasp.repository.postgres.tables.{SQLSinkTableDefinition, TableDefinition}

case class SQLSinkBLImpl(waspDB: WaspPostgresDB) extends SQLSinkBL with PostgresBL {

  implicit val tableDefinition: TableDefinition[SQLSinkModel, String] = SQLSinkTableDefinition

  override def getByName(name: String): Option[SQLSinkModel] = waspDB.getByPrimaryKey(name)

  override def getAll(): Seq[SQLSinkModel] = waspDB.getAll()

  override def persist(model: SQLSinkModel): Unit = waspDB.insert(model)

  override def upsert(model: SQLSinkModel): Unit = waspDB.upsert(model)

  override def createTable(): Unit = waspDB.createTable()

  override def deleteByName(name: String): Unit = waspDB.deleteByPrimaryKey(name)

}
