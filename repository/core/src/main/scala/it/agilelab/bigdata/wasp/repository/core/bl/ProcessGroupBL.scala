package it.agilelab.bigdata.wasp.repository.core.bl

import it.agilelab.bigdata.wasp.models.ProcessGroupModel

trait ProcessGroupBL {

  def getById(pgId: String): Option[ProcessGroupModel]

  def insert(versionedProcessGroup: ProcessGroupModel): Unit

  def upsert(versionedProcessGroup: ProcessGroupModel): Unit
}
