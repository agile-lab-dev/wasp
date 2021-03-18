package it.agilelab.bigdata.wasp.repository.core.bl

import it.agilelab.bigdata.wasp.models.CdcModel

trait CdcBL {

  def getByName(name: String): Option[CdcModel]

  def persist(cdcModel: CdcModel): Unit

  def upsert(cdcModel: CdcModel): Unit

  def getAll() : Seq[CdcModel]

}
