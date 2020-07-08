package it.agilelab.bigdata.wasp.core.bl

import it.agilelab.bigdata.wasp.core.models.FreeCodeModel

trait FreeCodeBL {

  def getByName(name: String): Option[FreeCodeModel]

  def deleteByName(name : String) : Unit

  def getAll: Seq[FreeCodeModel]

  def insert(freeCodeModel: FreeCodeModel): Unit

}


