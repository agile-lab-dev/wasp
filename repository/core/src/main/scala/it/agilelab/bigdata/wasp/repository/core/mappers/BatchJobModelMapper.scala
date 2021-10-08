package it.agilelab.bigdata.wasp.repository.core.mappers

import it.agilelab.bigdata.wasp.models.{BatchJobInstanceModel, BatchJobModel}
import it.agilelab.bigdata.wasp.repository.core.dbModels.{
  BatchJobDBModel,
  BatchJobDBModelV1,
  BatchJobInstanceDBModel,
  BatchJobInstanceDBModelV1
}

object BatchJobModelMapperSelector extends MapperSelector[BatchJobModel, BatchJobDBModel] {

  override def select(model: BatchJobDBModel): Mapper[BatchJobModel, BatchJobDBModel] = {

    model match {
      case _: BatchJobDBModelV1 => BatchJobMapperV1
      case o                    => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")
    }
  }

  def applyMap(p: BatchJobDBModel): BatchJobModel = {
    val mapper = select(p)
    mapper.fromDBModelToModel(p)
  }
}

object BatchJobMapperV1 extends Mapper[BatchJobModel, BatchJobDBModelV1] {
  override val version = "batchV1"

  override def fromModelToDBModel(p: BatchJobModel): BatchJobDBModelV1 = {

    val values      = BatchJobModel.unapply(p).get
    val makeDBModel = (BatchJobDBModelV1.apply _).tupled
    makeDBModel(values)
  }

  override def fromDBModelToModel[B >: BatchJobDBModelV1](p: B): BatchJobModel = {

    val values       = BatchJobDBModelV1.unapply(p.asInstanceOf[BatchJobDBModelV1]).get
    val makeProducer = (BatchJobModel.apply _).tupled
    makeProducer(values)
  }
}

object BatchJobInstanceModelMapperSelector extends MapperSelector[BatchJobInstanceModel, BatchJobInstanceDBModel] {

  override def select(model: BatchJobInstanceDBModel): Mapper[BatchJobInstanceModel, BatchJobInstanceDBModel] = {

    model match {
      case _: BatchJobInstanceDBModelV1 => BatchJobInstanceMapperV1
      case o                            => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")
    }
  }

  def applyMap(p: BatchJobInstanceDBModel): BatchJobInstanceModel = {
    val mapper = select(p)
    mapper.fromDBModelToModel(p)
  }
}

object BatchJobInstanceMapperV1 extends Mapper[BatchJobInstanceModel, BatchJobInstanceDBModelV1] {
  override val version = "batchInstanceV1"

  override def fromModelToDBModel(p: BatchJobInstanceModel): BatchJobInstanceDBModelV1 = {

    val values      = BatchJobInstanceModel.unapply(p).get
    val makeDBModel = (BatchJobInstanceDBModelV1.apply _).tupled
    makeDBModel(values)
  }

  override def fromDBModelToModel[B >: BatchJobInstanceDBModelV1](p: B): BatchJobInstanceModel = {

    val values       = BatchJobInstanceDBModelV1.unapply(p.asInstanceOf[BatchJobInstanceDBModelV1]).get
    val makeProducer = (BatchJobInstanceModel.apply _).tupled
    makeProducer(values)
  }
}
