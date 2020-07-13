package it.agile.bigdata.wasp.repository.postgres.bl


import it.agile.bigdata.wasp.repository.postgres.WaspPostgresDB
import it.agile.bigdata.wasp.repository.postgres.tables.{FreeCodeTableDefinition, TableDefinition}
import it.agilelab.bigdata.wasp.models.FreeCodeModel
import it.agilelab.bigdata.wasp.repository.core.bl.FreeCodeBL

case class FreeCodeBLImpl(waspDB : WaspPostgresDB) extends FreeCodeBL with PostgresBL  {

  implicit val tableDefinition: TableDefinition[FreeCodeModel,String] = FreeCodeTableDefinition

  override def createTable() : Unit = waspDB.createTable()

  override def getByName(name: String): Option[FreeCodeModel] = waspDB.getByPrimaryKey(name)

  override def deleteByName(name: String): Unit = waspDB.deleteByPrimaryKey(name)

  override def getAll: Seq[FreeCodeModel] = waspDB.getAll()

  override def insert(freeCodeModel: FreeCodeModel): Unit = waspDB.insert(freeCodeModel)

}

