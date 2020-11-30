package it.agilelab.bigdata.wasp.repository.core.bl

import it.agilelab.bigdata.wasp.models.HttpModel

trait HttpBL {
  def getByName(name: String): Option[HttpModel]

  def persist(HttpModel: HttpModel): Unit

  def getAll(): Seq[HttpModel]

  def upsert(HttpModel: HttpModel): Unit

  def insertIfNotExists(HttpModel: HttpModel): Unit
  
}
