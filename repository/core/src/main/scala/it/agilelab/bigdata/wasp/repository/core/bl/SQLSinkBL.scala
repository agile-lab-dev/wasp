package it.agilelab.bigdata.wasp.repository.core.bl

import it.agilelab.bigdata.wasp.models.SQLSinkModel

trait SQLSinkBL {
  def getByName(name: String): Option[SQLSinkModel]
  def getAll(): Seq[SQLSinkModel]
  def persist(model: SQLSinkModel): Unit
  def upsert(model: SQLSinkModel): Unit
  def deleteByName(name: String): Unit
}
