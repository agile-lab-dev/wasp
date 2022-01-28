package it.agilelab.bigdata.wasp.repository.core.mappers

import it.agilelab.bigdata.wasp.models.FreeCodeModel
import it.agilelab.bigdata.wasp.repository.core.dbModels.{FreeCodeDBModel, FreeCodeDBModelV1}

object FreeCodeMapperSelector extends MapperSelector[FreeCodeModel, FreeCodeDBModel]

object FreeCodeMapperV1 extends SimpleMapper[FreeCodeModel, FreeCodeDBModelV1] {
  override val version = "freeCodeV1"
  override def fromDBModelToModel[B >: FreeCodeDBModelV1](m: B): FreeCodeModel = m match {
    case mm: FreeCodeDBModelV1 => transform[FreeCodeModel](mm)
    case o                     => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")
  }}
