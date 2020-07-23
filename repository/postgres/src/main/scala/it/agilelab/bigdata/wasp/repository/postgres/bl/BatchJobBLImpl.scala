package it.agilelab.bigdata.wasp.repository.postgres.bl

import it.agilelab.bigdata.wasp.repository.postgres.tables.BatchJobTableDefinition
import it.agilelab.bigdata.wasp.models.BatchJobModel
import it.agilelab.bigdata.wasp.repository.core.bl.{BatchJobBL, BatchJobInstanceBL}
import it.agilelab.bigdata.wasp.repository.postgres.WaspPostgresDB
import it.agilelab.bigdata.wasp.repository.postgres.tables.{BatchJobTableDefinition, TableDefinition}


case class BatchJobBLImpl(waspDB: WaspPostgresDB ) extends BatchJobBL with PostgresBL {

  implicit val tableDefinition: TableDefinition[BatchJobModel,String] = BatchJobTableDefinition

  override def getByName(name: String): Option[BatchJobModel] = waspDB.getByPrimaryKey(name)

  override def getAll: Seq[BatchJobModel] = waspDB.getAll()

  override def update(batchJobModel: BatchJobModel): Unit = waspDB.updateByPrimaryKey(batchJobModel)

  override def insert(batchJobModel: BatchJobModel): Unit = waspDB.insert(batchJobModel)

  override def upsert(batchJobModel: BatchJobModel): Unit = waspDB.upsert(batchJobModel)

  override def deleteByName(name: String): Unit =  waspDB.deleteByPrimaryKey(name)

  override def instances(): BatchJobInstanceBL = BatchJobInstanceBLImpl(waspDB)

}
