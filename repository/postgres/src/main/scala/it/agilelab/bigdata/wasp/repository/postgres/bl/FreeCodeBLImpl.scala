package it.agilelab.bigdata.wasp.repository.postgres.bl

import it.agilelab.bigdata.wasp.repository.postgres.tables.FreeCodeTableDefinition
import it.agilelab.bigdata.wasp.models.FreeCodeModel
import it.agilelab.bigdata.wasp.repository.core.bl.FreeCodeBL
import it.agilelab.bigdata.wasp.repository.postgres.WaspPostgresDB
import it.agilelab.bigdata.wasp.repository.postgres.tables.{FreeCodeTableDefinition, TableDefinition}

case class FreeCodeBLImpl(waspDB : WaspPostgresDB) extends FreeCodeBL with PostgresBL  {

  implicit val tableDefinition: TableDefinition[FreeCodeModel,String] = FreeCodeTableDefinition

  override def createTable() : Unit = waspDB.createTable()

  override def getByName(name: String): Option[FreeCodeModel] = waspDB.getByPrimaryKey(name)

  override def deleteByName(name: String): Unit = waspDB.deleteByPrimaryKey(name)

  override def getAll: Seq[FreeCodeModel] = waspDB.getAll()

  override def insert(freeCodeModel: FreeCodeModel): Unit = waspDB.insert(freeCodeModel)

  override def upsert(freeCodeModel: FreeCodeModel): Unit = waspDB.upsert(freeCodeModel)

}

