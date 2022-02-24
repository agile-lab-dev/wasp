package it.agilelab.bigdata.wasp.repository.core.mappers

import it.agilelab.bigdata.wasp.models.KeyValueModel
import it.agilelab.bigdata.wasp.repository.core.dbModels.{KeyValueDBModel, KeyValueDBModelV1}

object KeyValueMapperSelector extends MapperSelector[KeyValueModel, KeyValueDBModel]

object KeyValueMapperV1 extends SimpleMapper[KeyValueModel, KeyValueDBModelV1] {
  override val version = "keyValueV1"
  override def fromDBModelToModel[B >: KeyValueDBModelV1](m: B): KeyValueModel = m match {
    case mm: KeyValueDBModelV1 => transform[KeyValueModel](mm)
    case o                     => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")
  }
}
