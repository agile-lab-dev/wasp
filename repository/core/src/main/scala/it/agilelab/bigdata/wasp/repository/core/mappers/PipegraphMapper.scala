package it.agilelab.bigdata.wasp.repository.core.mappers

import it.agilelab.bigdata.wasp.models.{PipegraphInstanceModel, PipegraphModel}
import it.agilelab.bigdata.wasp.repository.core.dbModels.{PipegraphDBModel, PipegraphDBModelV1, PipegraphInstanceDBModel, PipegraphInstanceDBModelV1}

object PipegraphDBModelMapperSelector extends MapperSelector[PipegraphModel, PipegraphDBModel]

object PipegraphInstanceDBModelMapperSelector extends MapperSelector[PipegraphInstanceModel, PipegraphInstanceDBModel]

object PipegraphMapperV1 extends SimpleMapper[PipegraphModel, PipegraphDBModelV1]{
  val version = "PipegraphV1"
  override def fromDBModelToModel[B >: PipegraphDBModelV1](m: B): PipegraphModel = m match {
    case mm: PipegraphDBModelV1 => transform[PipegraphModel](mm)
    case o                     => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")
  }
}

object PipegraphInstanceMapperV1 extends SimpleMapper[PipegraphInstanceModel, PipegraphInstanceDBModelV1]{
  val version = "PipegraphInstanceV1"
  override def fromDBModelToModel[B >: PipegraphInstanceDBModelV1](m: B): PipegraphInstanceModel = m match {
    case mm: PipegraphInstanceDBModelV1 => transform[PipegraphInstanceModel](mm)
    case o                     => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")
  }
}


