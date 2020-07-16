package it.agile.bigdata.wasp.repository.postgres.bl

import it.agile.bigdata.wasp.repository.postgres.WaspPostgresDB
import it.agile.bigdata.wasp.repository.postgres.tables.{BatchSchedulersTableDefinition, TableDefinition}
import it.agilelab.bigdata.wasp.models.BatchSchedulerModel
import it.agilelab.bigdata.wasp.repository.core.bl.BatchSchedulersBL

case class BatchSchedulersBLImpl(waspDB: WaspPostgresDB) extends BatchSchedulersBL with PostgresBL {

  override implicit val tableDefinition: TableDefinition[BatchSchedulerModel,String] = BatchSchedulersTableDefinition

  override def getActiveSchedulers(isActive: Boolean): Seq[BatchSchedulerModel] = waspDB.getBy(s"${BatchSchedulersTableDefinition.isActive}=$isActive")

  override def persist(schedulerModel: BatchSchedulerModel): Unit = waspDB.insert(schedulerModel)

}
