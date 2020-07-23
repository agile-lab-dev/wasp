package it.agilelab.bigdata.wasp.repository.postgres.bl

import it.agilelab.bigdata.wasp.repository.postgres.tables.TableDefinition
import it.agilelab.bigdata.wasp.models.IndexModel
import it.agilelab.bigdata.wasp.repository.core.bl.IndexBL
import it.agilelab.bigdata.wasp.repository.postgres.WaspPostgresDB
import it.agilelab.bigdata.wasp.repository.postgres.tables.{IndexTableDefinition, TableDefinition}

case class IndexBLImpl(waspDB: WaspPostgresDB ) extends IndexBL with PostgresBL {

  implicit val tableDefinition: TableDefinition[IndexModel,String] = IndexTableDefinition

  override def getByName(name: String): Option[IndexModel] = waspDB.getByPrimaryKey(name)

  override def persist(indexModel: IndexModel): Unit = waspDB.insert(indexModel)

  override def getAll(): Seq[IndexModel] = waspDB.getAll()

  override def upsert(indexModel: IndexModel): Unit = waspDB.upsert(indexModel)

  override def insertIfNotExists(indexModel: IndexModel): Unit = waspDB.insertIfNotExists(indexModel)

}
