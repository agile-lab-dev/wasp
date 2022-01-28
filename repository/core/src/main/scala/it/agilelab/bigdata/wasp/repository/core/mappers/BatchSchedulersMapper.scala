package it.agilelab.bigdata.wasp.repository.core.mappers

import it.agilelab.bigdata.wasp.models.BatchSchedulerModel
import it.agilelab.bigdata.wasp.repository.core.dbModels.{BatchSchedulerDBModel, BatchSchedulerDBModelV1}

object BatchSchedulersMapperSelector extends MapperSelector[BatchSchedulerModel, BatchSchedulerDBModel]

object BatchSchedulerMapperV1 extends SimpleMapper[BatchSchedulerModel, BatchSchedulerDBModelV1] {
  override val version = "batchSchedulerV1"
  override def fromDBModelToModel[B >: BatchSchedulerDBModelV1](m: B): BatchSchedulerModel = m match {
    case mm: BatchSchedulerDBModelV1 => transform[BatchSchedulerModel](mm)
    case o                     => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")
  }
}
