package it.agilelab.bigdata.wasp.repository.postgres.bl

import it.agilelab.bigdata.wasp.models.ProcessGroupModel
import it.agilelab.bigdata.wasp.repository.core.bl.ProcessGroupBL
import it.agilelab.bigdata.wasp.repository.postgres.WaspPostgresDB
import it.agilelab.bigdata.wasp.repository.postgres.tables.ProcessGroupTableDefinition

case class ProcessGroupBLImpl(waspDB: WaspPostgresDB ) extends ProcessGroupBL with PostgresBL {

  override implicit val tableDefinition = ProcessGroupTableDefinition

  override def getById(pgId: String): Option[ProcessGroupModel] = waspDB.getByPrimaryKey(pgId)

  override def insert(versionedProcessGroup: ProcessGroupModel): Unit = waspDB.insert(versionedProcessGroup)

  override def upsert(versionedProcessGroup: ProcessGroupModel): Unit = waspDB.upsert(versionedProcessGroup)
}
