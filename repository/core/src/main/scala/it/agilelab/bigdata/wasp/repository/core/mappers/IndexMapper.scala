package it.agilelab.bigdata.wasp.repository.core.mappers

import it.agilelab.bigdata.wasp.models.IndexModel
import it.agilelab.bigdata.wasp.repository.core.dbModels.{IndexDBModel, IndexDBModelV1}

object IndexDBModelMapperSelector extends MapperSelector[IndexModel, IndexDBModel]

object IndexMapperV1 extends SimpleMapper[IndexModel, IndexDBModelV1] {
  override val version = "indexV1"
  override def fromDBModelToModel[B >: IndexDBModelV1](m: B): IndexModel = m match {
    case mm: IndexDBModelV1 => transform[IndexModel](mm)
    case o                     => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")
  }}
