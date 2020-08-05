package it.agilelab.bigdata.wasp.repository.postgres.bl

import it.agilelab.bigdata.wasp.repository.postgres.tables.PipegraphTableDefinition
import it.agilelab.bigdata.wasp.models.PipegraphStatus.PipegraphStatus
import it.agilelab.bigdata.wasp.models.{PipegraphModel, PipegraphStatus}
import it.agilelab.bigdata.wasp.repository.core.bl.{PipegraphBL, PipegraphInstanceBl}
import it.agilelab.bigdata.wasp.repository.postgres.WaspPostgresDB
import it.agilelab.bigdata.wasp.repository.postgres.tables.{PipegraphTableDefinition, TableDefinition}

case class PipegraphBLImpl(waspDB : WaspPostgresDB) extends PipegraphBL with PostgresBL {

  implicit val tableDefinition: TableDefinition[PipegraphModel,String] = PipegraphTableDefinition

  private lazy val _instances = PipegraphInstanceBlImpl(waspDB)

  override def getByName(name: String): Option[PipegraphModel] = waspDB.getByPrimaryKey(name)

  override def getAll: Seq[PipegraphModel] = waspDB.getAll()

  override def getSystemPipegraphs: Seq[PipegraphModel] = waspDB.getBy(Array(PipegraphTableDefinition.isSystem -> true))

  override def getNonSystemPipegraphs: Seq[PipegraphModel] = waspDB.getBy(Array(PipegraphTableDefinition.isSystem -> false))

  override def getActivePipegraphs(): Seq[PipegraphModel] = {
    val allowedStates: Set[PipegraphStatus] = Set(PipegraphStatus.PENDING, PipegraphStatus.PROCESSING)
    _instances
      .all()
      .filter(instance => allowedStates.contains(instance.status))
      .flatMap(instance => getByName(instance.name))
  }

  override def insert(pipegraph: PipegraphModel): Unit = waspDB.insert(pipegraph)

  override def insertIfNotExists(pipegraph: PipegraphModel): Unit = waspDB.insertIfNotExists(pipegraph)

  override def upsert(pipegraph: PipegraphModel): Unit = waspDB.upsert(pipegraph)

  override def update(pipegraphModel: PipegraphModel): Unit = waspDB.updateByPrimaryKey(pipegraphModel)

  override def deleteByName(id_string: String): Unit = waspDB.deleteByPrimaryKey(id_string)

  override def instances(): PipegraphInstanceBl = _instances

}
