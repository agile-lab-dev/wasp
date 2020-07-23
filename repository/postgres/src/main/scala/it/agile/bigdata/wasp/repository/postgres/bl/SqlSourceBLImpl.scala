package it.agile.bigdata.wasp.repository.postgres.bl

import it.agile.bigdata.wasp.repository.postgres.WaspPostgresDB
import it.agilelab.bigdata.wasp.models.SqlSourceModel
import it.agilelab.bigdata.wasp.repository.core.bl.SqlSourceBl
import it.agile.bigdata.wasp.repository.postgres.tables.{SqlSourceTableDefinition, TableDefinition}


case class SqlSourceBLImpl(waspDB: WaspPostgresDB) extends SqlSourceBl with PostgresBL  {

  implicit val tableDefinition: TableDefinition[SqlSourceModel,String] = SqlSourceTableDefinition

  override def getByName(name: String): Option[SqlSourceModel] =  waspDB.getByPrimaryKey(name)

  override def persist(sqlSourceModel: SqlSourceModel): Unit =  waspDB.insert(sqlSourceModel)

  override def upsert(sqlSourceModel: SqlSourceModel): Unit = waspDB.upsert(sqlSourceModel)

  override def createTable(): Unit = waspDB.createTable()
}
