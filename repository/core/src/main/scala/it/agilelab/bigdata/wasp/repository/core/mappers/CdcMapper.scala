package it.agilelab.bigdata.wasp.repository.core.mappers

import it.agilelab.bigdata.wasp.models.CdcModel
import it.agilelab.bigdata.wasp.repository.core.dbModels.{CdcDBModel, CdcDBModelV1}

object CdcMapperSelector extends MapperSelector[CdcModel, CdcDBModel]

object CdcMapperV1 extends SimpleMapper[CdcModel, CdcDBModelV1] {
  override val version = "cdcV1"
  override def fromDBModelToModel[B >: CdcDBModelV1](m: B): CdcModel = m match {
    case mm: CdcDBModelV1 => transform[CdcModel](mm)
    case o                     => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")
  }
}
