package it.agilelab.bigdata.wasp.repository.core.mappers

import it.agilelab.bigdata.wasp.models.BatchSchedulerModel
import it.agilelab.bigdata.wasp.repository.core.dbModels.{BatchSchedulerDBModel, BatchSchedulerDBModelV1}

object BatchSchedulersMapperSelector extends MapperSelector[BatchSchedulerModel, BatchSchedulerDBModel]{

  override def select(model : BatchSchedulerDBModel) : Mapper[BatchSchedulerModel, BatchSchedulerDBModelV1] = {

    model match {
      case _: BatchSchedulerDBModelV1 => BatchSchedulerMapperV1
      case _ => throw new Exception("There is no available mapper for this DBModel, create one!")
    }
  }

  def applyMap(p: BatchSchedulerDBModel) : BatchSchedulerModel = {
    val mapper = select(p)
    mapper.fromDBModelToModel(p)
  }
}

object BatchSchedulerMapperV1 extends Mapper[BatchSchedulerModel, BatchSchedulerDBModelV1] {
  override val version = "batchSchedulerV1"

  override def fromModelToDBModel(p: BatchSchedulerModel): BatchSchedulerDBModelV1 = {

    val values = BatchSchedulerModel.unapply(p).get
    val makeDBModel = (BatchSchedulerDBModelV1.apply _).tupled
    makeDBModel(values)
  }


  override def fromDBModelToModel[B >: BatchSchedulerDBModelV1](p: B): BatchSchedulerModel = {

    val values = BatchSchedulerDBModelV1.unapply(p.asInstanceOf[BatchSchedulerDBModelV1]).get
    val makeProducer = (BatchSchedulerModel.apply _).tupled
    makeProducer(values)
  }
}