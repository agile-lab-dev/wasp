package it.agilelab.bigdata.wasp.repository.core.bl

import it.agilelab.bigdata.wasp.models.SqlSourceModel

trait SqlSourceBl {

  def getByName(name: String): Option[SqlSourceModel]

  def persist(rawModel: SqlSourceModel): Unit

  def upsert(rawModel: SqlSourceModel): Unit
}