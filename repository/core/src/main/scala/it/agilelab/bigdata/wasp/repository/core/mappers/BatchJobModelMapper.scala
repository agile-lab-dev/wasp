package it.agilelab.bigdata.wasp.repository.core.mappers

import it.agilelab.bigdata.wasp.models.{BatchJobInstanceModel, BatchJobModel}
import it.agilelab.bigdata.wasp.repository.core.dbModels.{
  BatchJobDBModel,
  BatchJobDBModelV1,
  BatchJobInstanceDBModel,
  BatchJobInstanceDBModelV1
}

object BatchJobModelMapperSelector extends MapperSelector[BatchJobModel, BatchJobDBModel]

object BatchJobMapperV1 extends SimpleMapper[BatchJobModel, BatchJobDBModelV1] {
  override val version = "batchV1"
  override def fromDBModelToModel[B >: BatchJobDBModelV1](m: B): BatchJobModel = m match {
    case mm: BatchJobDBModelV1 => transform[BatchJobModel](mm)
    case o                     => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")
  }
}

object BatchJobInstanceModelMapperSelector extends MapperSelector[BatchJobInstanceModel, BatchJobInstanceDBModel]

object BatchJobInstanceMapperV1 extends SimpleMapper[BatchJobInstanceModel, BatchJobInstanceDBModelV1] {
  override val version = "batchInstanceV1"
  override def fromDBModelToModel[B >: BatchJobInstanceDBModelV1](m: B): BatchJobInstanceModel = m match {
    case mm: BatchJobInstanceDBModelV1 => transform[BatchJobInstanceModel](mm)
    case o                     => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")
  }
}
