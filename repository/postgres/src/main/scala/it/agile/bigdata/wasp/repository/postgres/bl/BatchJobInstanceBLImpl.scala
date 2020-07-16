package it.agile.bigdata.wasp.repository.postgres.bl

import it.agile.bigdata.wasp.repository.postgres.WaspPostgresDB
import it.agile.bigdata.wasp.repository.postgres.tables.{BatchJobInstanceTableDefinition, TableDefinition}
import it.agilelab.bigdata.wasp.models.BatchJobInstanceModel
import it.agilelab.bigdata.wasp.repository.core.bl.BatchJobInstanceBL

case class BatchJobInstanceBLImpl(waspDB: WaspPostgresDB) extends BatchJobInstanceBL with PostgresBL {

  implicit val tableDefinition: TableDefinition[BatchJobInstanceModel,String] = BatchJobInstanceTableDefinition

  override def getByName(name: String): Option[BatchJobInstanceModel] = waspDB.getByPrimaryKey(name)

  override def insert(instance: BatchJobInstanceModel): BatchJobInstanceModel = {
    waspDB.insert(instance)
    instance
  }

  override def update(instance: BatchJobInstanceModel): BatchJobInstanceModel = {
    waspDB.updateByPrimaryKey(instance)
    instance
  }

  override def all(): Seq[BatchJobInstanceModel] = waspDB.getAll()

  override def instancesOf(name: String): Seq[BatchJobInstanceModel] =
    waspDB.getBy(s"${BatchJobInstanceTableDefinition.instanceOf}='$name'")

  override def createTable(): Unit = waspDB.createTable()
}
