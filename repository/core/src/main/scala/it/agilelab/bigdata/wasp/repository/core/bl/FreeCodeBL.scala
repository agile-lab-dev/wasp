package it.agilelab.bigdata.wasp.repository.core.bl

import it.agilelab.bigdata.wasp.models.FreeCodeModel

trait FreeCodeBL {

  def getByName(name: String): Option[FreeCodeModel]

  def deleteByName(name : String) : Unit

  def getAll: Seq[FreeCodeModel]

  def insert(freeCodeModel: FreeCodeModel): Unit

  def upsert(freeCodeModel: FreeCodeModel): Unit
}


