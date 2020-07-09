package it.agilelab.bigdata.wasp.repository.core.bl

import it.agilelab.bigdata.wasp.models.DocumentModel

trait DocumentBL {

  def getByName(name: String): Option[DocumentModel]
  def getAll(): Seq[DocumentModel]
  def persist(rawModel: DocumentModel): Unit
  def upsert(rawModel: DocumentModel): Unit
}

