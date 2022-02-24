package it.agilelab.bigdata.wasp.repository.core.mappers

import it.agilelab.bigdata.wasp.models.HttpModel
import it.agilelab.bigdata.wasp.repository.core.dbModels.{HttpDBModel, HttpDBModelV1}

object HttpDBModelMapperSelector extends MapperSelector[HttpModel, HttpDBModel]

object HttpMapperV1 extends SimpleMapper[HttpModel, HttpDBModelV1] {
  override val version = "httpV1"
  override def fromDBModelToModel[B >: HttpDBModelV1](m: B): HttpModel = m match {
    case mm: HttpDBModelV1 => transform[HttpModel](mm)
    case o                     => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")
  }}
