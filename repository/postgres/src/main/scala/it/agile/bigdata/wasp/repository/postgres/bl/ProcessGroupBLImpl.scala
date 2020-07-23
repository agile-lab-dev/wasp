package it.agile.bigdata.wasp.repository.postgres.bl

import it.agile.bigdata.wasp.repository.postgres.WaspPostgresDB
import it.agile.bigdata.wasp.repository.postgres.tables.ProcessGroupTableDefinition
import it.agilelab.bigdata.wasp.models.ProcessGroupModel
import it.agilelab.bigdata.wasp.repository.core.bl.ProcessGroupBL

case class ProcessGroupBLImpl(waspDB: WaspPostgresDB ) extends ProcessGroupBL with PostgresBL {

  override implicit val tableDefinition = ProcessGroupTableDefinition

  override def getById(pgId: String): Option[ProcessGroupModel] = waspDB.getByPrimaryKey(pgId)

  override def insert(versionedProcessGroup: ProcessGroupModel): Unit = waspDB.insert(versionedProcessGroup)

}
