package it.agilelab.bigdata.wasp.core.bl

import it.agilelab.bigdata.wasp.core.models.ProcessGroupModel

trait ProcessGroupBL {

  def getById(pgId: String): Option[ProcessGroupModel]

  def insert(versionedProcessGroup: ProcessGroupModel): Unit

}
