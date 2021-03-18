package it.agilelab.bigdata.wasp.repository.postgres.bl

import it.agilelab.bigdata.wasp.models.CdcModel
import it.agilelab.bigdata.wasp.repository.core.bl.CdcBL
import it.agilelab.bigdata.wasp.repository.postgres.WaspPostgresDB
import it.agilelab.bigdata.wasp.repository.postgres.tables.{CdcTableDefinition, TableDefinition}

case class CdcBLImpl(waspDB: WaspPostgresDB )  extends CdcBL with PostgresBL {

  implicit val tableDefinition: TableDefinition[CdcModel,String] = CdcTableDefinition

  override def getByName(name: String): Option[CdcModel] = waspDB.getByPrimaryKey(name)

  override def persist(cdcModel: CdcModel): Unit = waspDB.insert(cdcModel)

  override def upsert(cdcModel: CdcModel): Unit = waspDB.upsert(cdcModel)

  override def getAll(): Seq[CdcModel] = waspDB.getAll()

  override def createTable(): Unit = waspDB.createTable()

}
