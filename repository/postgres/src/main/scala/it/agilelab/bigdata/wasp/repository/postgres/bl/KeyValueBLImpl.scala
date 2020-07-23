package it.agilelab.bigdata.wasp.repository.postgres.bl

import it.agilelab.bigdata.wasp.repository.postgres.tables.KeyValueTableDefinition
import it.agilelab.bigdata.wasp.models.KeyValueModel
import it.agilelab.bigdata.wasp.repository.core.bl.KeyValueBL
import it.agilelab.bigdata.wasp.repository.postgres.WaspPostgresDB
import it.agilelab.bigdata.wasp.repository.postgres.tables.{KeyValueTableDefinition, TableDefinition}

case class KeyValueBLImpl(waspDB: WaspPostgresDB ) extends KeyValueBL with PostgresBL {

  implicit val tableDefinition: TableDefinition[KeyValueModel, String] = KeyValueTableDefinition

  override def getByName(name: String): Option[KeyValueModel] = waspDB.getByPrimaryKey(name)

  override def getAll(): Seq[KeyValueModel] = waspDB.getAll()

  override def persist(keyValueModel: KeyValueModel): Unit = waspDB.insert(keyValueModel)

  override def upsert(keyValueModel: KeyValueModel): Unit = waspDB.upsert(keyValueModel)

  override def createTable(): Unit = waspDB.createTable()
}