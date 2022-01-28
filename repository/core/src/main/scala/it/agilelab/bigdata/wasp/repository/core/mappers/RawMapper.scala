package it.agilelab.bigdata.wasp.repository.core.mappers

import it.agilelab.bigdata.wasp.models.RawModel
import it.agilelab.bigdata.wasp.repository.core.dbModels.{RawDBModel, RawDBModelV1}

object RawMapperSelector extends MapperSelector[RawModel, RawDBModel]

object RawMapperV1 extends SimpleMapper[RawModel, RawDBModelV1] {
  override val version = "rawV1"
  override def fromDBModelToModel[B >: RawDBModelV1](m: B): RawModel = m match {
    case mm: RawDBModelV1 => transform[RawModel](mm)
    case o                     => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")
  }
}
