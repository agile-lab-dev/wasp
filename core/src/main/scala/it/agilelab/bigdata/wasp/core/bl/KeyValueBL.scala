package it.agilelab.bigdata.wasp.core.bl

import it.agilelab.bigdata.wasp.core.models.KeyValueModel

trait KeyValueBL {

  def getByName(name: String): Option[KeyValueModel]

  def getAll(): Seq[KeyValueModel]

  def persist(rawModel: KeyValueModel): Unit

  def upsert(rawModel: KeyValueModel): Unit
}