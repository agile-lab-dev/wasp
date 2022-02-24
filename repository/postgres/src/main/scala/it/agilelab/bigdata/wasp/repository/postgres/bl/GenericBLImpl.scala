package it.agilelab.bigdata.wasp.repository.postgres.bl

import it.agilelab.bigdata.wasp.models.GenericModel
import it.agilelab.bigdata.wasp.repository.core.bl.GenericBL
import it.agilelab.bigdata.wasp.repository.postgres.WaspPostgresDB
import it.agilelab.bigdata.wasp.repository.postgres.tables.{GenericTableDefinition, TableDefinition}

case class GenericBLImpl(waspDB: WaspPostgresDB) extends GenericBL with PostgresBL {
  override implicit val tableDefinition: TableDefinition[GenericModel, String] = GenericTableDefinition
  override def getByName(name: String): Option[GenericModel] = waspDB.getByPrimaryKey(name)

  override def persist(model: GenericModel): Unit = waspDB.insert(model)

  override def getAll(): Seq[GenericModel] = waspDB.getAll()

  override def upsert(model: GenericModel): Unit = waspDB.upsert(model)
}
