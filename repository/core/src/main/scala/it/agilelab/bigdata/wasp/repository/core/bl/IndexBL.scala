package it.agilelab.bigdata.wasp.repository.core.bl

import it.agilelab.bigdata.wasp.models.IndexModel

trait IndexBL {
  def getByName(name: String): Option[IndexModel]

  def persist(indexModel: IndexModel): Unit

  def getAll(): Seq[IndexModel]

  def upsert(indexModel: IndexModel): Unit

  def insertIfNotExists(indexModel: IndexModel): Unit

}