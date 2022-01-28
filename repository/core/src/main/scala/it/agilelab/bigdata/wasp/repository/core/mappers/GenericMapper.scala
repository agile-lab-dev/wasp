package it.agilelab.bigdata.wasp.repository.core.mappers

import it.agilelab.bigdata.wasp.models.GenericModel
import it.agilelab.bigdata.wasp.repository.core.dbModels.{GenericDBModel, GenericDBModelV1}

object GenericMapperSelector extends MapperSelector[GenericModel, GenericDBModel]

object GenericMapperV1 extends SimpleMapper[GenericModel, GenericDBModelV1] {
  override val version = "genericV1"
  override def fromDBModelToModel[B >: GenericDBModelV1](m: B): GenericModel = m match {
    case mm: GenericDBModelV1 => transform[GenericModel](mm)
    case o                     => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")
  }}
