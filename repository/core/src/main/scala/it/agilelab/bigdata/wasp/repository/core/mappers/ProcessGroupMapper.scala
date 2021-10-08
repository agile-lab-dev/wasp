package it.agilelab.bigdata.wasp.repository.core.mappers

import it.agilelab.bigdata.wasp.models.ProcessGroupModel
import it.agilelab.bigdata.wasp.repository.core.dbModels.{ProcessGroupDBModel, ProcessGroupDBModelV1}

object ProcessGroupMapperSelector extends MapperSelector[ProcessGroupModel, ProcessGroupDBModel] {

  override def select(model: ProcessGroupDBModel): Mapper[ProcessGroupModel, ProcessGroupDBModel] = {

    model match {
      case _: ProcessGroupDBModelV1 => ProcessGroupMapperV1
      case o                        => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")
    }
  }

  def applyMap(p: ProcessGroupDBModel): ProcessGroupModel = {
    val mapper = select(p)
    mapper.fromDBModelToModel(p)
  }
}

object ProcessGroupMapperV1 extends Mapper[ProcessGroupModel, ProcessGroupDBModelV1] {
  override val version = "processGroupV1"

  override def fromModelToDBModel(p: ProcessGroupModel): ProcessGroupDBModelV1 = {

    val values      = ProcessGroupModel.unapply(p).get
    val makeDBModel = (ProcessGroupDBModelV1.apply _).tupled
    makeDBModel(values)
  }

  override def fromDBModelToModel[B >: ProcessGroupDBModelV1](p: B): ProcessGroupModel = {

    val values       = ProcessGroupDBModelV1.unapply(p.asInstanceOf[ProcessGroupDBModelV1]).get
    val makeProducer = (ProcessGroupModel.apply _).tupled
    makeProducer(values)
  }
}
