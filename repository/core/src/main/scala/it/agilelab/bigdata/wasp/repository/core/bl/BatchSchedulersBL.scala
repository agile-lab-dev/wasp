package it.agilelab.bigdata.wasp.repository.core.bl

import it.agilelab.bigdata.wasp.models.BatchSchedulerModel

trait BatchSchedulersBL {

  def getActiveSchedulers(isActive: Boolean = true): Seq[BatchSchedulerModel]

  def persist(schedulerModel: BatchSchedulerModel): Unit
}