package it.agilelab.bigdata.wasp.core.bl

import it.agilelab.bigdata.wasp.core.models.BatchSchedulerModel

trait BatchSchedulersBL {

  def getActiveSchedulers(isActive: Boolean = true): Seq[BatchSchedulerModel]

  def persist(schedulerModel: BatchSchedulerModel): Unit
}