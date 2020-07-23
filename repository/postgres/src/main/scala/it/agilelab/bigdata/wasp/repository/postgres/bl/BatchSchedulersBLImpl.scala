package it.agilelab.bigdata.wasp.repository.postgres.bl

import it.agilelab.bigdata.wasp.repository.postgres.tables.TableDefinition
import it.agilelab.bigdata.wasp.models.BatchSchedulerModel
import it.agilelab.bigdata.wasp.repository.core.bl.BatchSchedulersBL
import it.agilelab.bigdata.wasp.repository.postgres.WaspPostgresDB
import it.agilelab.bigdata.wasp.repository.postgres.tables.{BatchSchedulersTableDefinition, TableDefinition}

case class BatchSchedulersBLImpl(waspDB: WaspPostgresDB) extends BatchSchedulersBL with PostgresBL {

  override implicit val tableDefinition: TableDefinition[BatchSchedulerModel,String] = BatchSchedulersTableDefinition

  override def getActiveSchedulers(isActive: Boolean): Seq[BatchSchedulerModel] = waspDB.getBy(Array((BatchSchedulersTableDefinition.isActive,isActive)))

  override def persist(schedulerModel: BatchSchedulerModel): Unit = waspDB.insert(schedulerModel)

}
