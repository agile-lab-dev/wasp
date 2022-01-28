package it.agilelab.bigdata.wasp.repository.core.mappers

import it.agilelab.bigdata.wasp.models.MlModelOnlyInfo
import it.agilelab.bigdata.wasp.repository.core.dbModels.{MlDBModelOnlyInfo, MlDBModelOnlyInfoV1}

object MlDBModelMapperSelector extends MapperSelector[MlModelOnlyInfo, MlDBModelOnlyInfo]

object MlDBModelMapperV1 extends SimpleMapper[MlModelOnlyInfo, MlDBModelOnlyInfoV1] {
  override val version = "mlModelV1"
  override def fromDBModelToModel[B >: MlDBModelOnlyInfoV1](m: B): MlModelOnlyInfo = m match {
    case mm: MlDBModelOnlyInfoV1 => transform[MlModelOnlyInfo](mm)
    case o                     => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")
  }
}
