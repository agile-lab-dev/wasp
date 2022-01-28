package it.agilelab.bigdata.wasp.repository.core.mappers

import it.agilelab.bigdata.wasp.models.ProcessGroupModel
import it.agilelab.bigdata.wasp.repository.core.dbModels.{ProcessGroupDBModel, ProcessGroupDBModelV1}

object ProcessGroupMapperSelector extends MapperSelector[ProcessGroupModel, ProcessGroupDBModel]

object ProcessGroupMapperV1 extends SimpleMapper[ProcessGroupModel, ProcessGroupDBModelV1] {
  override val version = "processGroupV1"
  override def fromDBModelToModel[B >: ProcessGroupDBModelV1](m: B): ProcessGroupModel = m match {
    case mm: ProcessGroupDBModelV1 => transform[ProcessGroupModel](mm)
    case o                     => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")
  }
}
