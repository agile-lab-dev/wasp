package it.agilelab.bigdata.wasp.repository.core.mappers

import it.agilelab.bigdata.wasp.models.DocumentModel
import it.agilelab.bigdata.wasp.repository.core.dbModels.{CdcDBModelV1, DocumentDBModel, DocumentDBModelV1}

object DocumentDBModelMapperSelector extends MapperSelector[DocumentModel, DocumentDBModel]

object DocumentMapperV1 extends SimpleMapper[DocumentModel, DocumentDBModelV1] {
  override val version = "docV1"
  override def fromDBModelToModel[B >: DocumentDBModelV1](m: B): DocumentModel = m match {
    case mm: DocumentDBModelV1 => transform[DocumentModel](mm)
    case o                     => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")
  }}
