package it.agilelab.bigdata.wasp.repository.postgres.bl

import it.agilelab.bigdata.wasp.repository.postgres.tables.PipegraphInstanceTableDefinition
import it.agilelab.bigdata.wasp.models.PipegraphInstanceModel
import it.agilelab.bigdata.wasp.repository.core.bl.PipegraphInstanceBl
import it.agilelab.bigdata.wasp.repository.postgres.WaspPostgresDB
import it.agilelab.bigdata.wasp.repository.postgres.tables.{PipegraphInstanceTableDefinition, TableDefinition}

case class PipegraphInstanceBlImpl(waspDB : WaspPostgresDB) extends PipegraphInstanceBl with PostgresBL {

  implicit val tableDefinition: TableDefinition[PipegraphInstanceModel,String] = PipegraphInstanceTableDefinition

  override def getByName(name: String): Option[PipegraphInstanceModel] = waspDB.getByPrimaryKey(name)

  override def insert(instance: PipegraphInstanceModel): PipegraphInstanceModel = {
    waspDB.insert(instance)
    instance
  }

  override def update(instance: PipegraphInstanceModel): PipegraphInstanceModel = {
    waspDB.updateByPrimaryKey(instance)
    instance
  }

  override def all(): Seq[PipegraphInstanceModel] = waspDB.getAll()

  override def instancesOf(name: String): Seq[PipegraphInstanceModel] = waspDB.getBy(Array(PipegraphInstanceTableDefinition.instanceOf -> name))

}
