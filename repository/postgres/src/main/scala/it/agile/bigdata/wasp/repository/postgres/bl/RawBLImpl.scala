package it.agile.bigdata.wasp.repository.postgres.bl

import it.agile.bigdata.wasp.repository.postgres.WaspPostgresDB
import it.agile.bigdata.wasp.repository.postgres.tables.{RawTableDefinition, TableDefinition}
import it.agilelab.bigdata.wasp.models.RawModel
import it.agilelab.bigdata.wasp.repository.core.bl.RawBL

case class RawBLImpl(waspDB: WaspPostgresDB )  extends RawBL with PostgresBL {

  implicit val tableDefinition: TableDefinition[RawModel,String] = RawTableDefinition

  override def getByName(name: String): Option[RawModel] = waspDB.getByPrimaryKey(name)

  override def persist(rawModel: RawModel): Unit = waspDB.insert(rawModel)

  override def upsert(rawModel: RawModel): Unit = waspDB.upsert(rawModel)

  override def getAll(): Seq[RawModel] = waspDB.getAll()

  override def createTable(): Unit = waspDB.createTable()

}
