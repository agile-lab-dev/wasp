package it.agilelab.bigdata.wasp.repository.core.mappers

import it.agilelab.bigdata.wasp.models.{DatastoreModel, MultiTopicModel}
import it.agilelab.bigdata.wasp.repository.core.dbModels.{MultiTopicDBModel, MultiTopicDBModelV1}

object MultiTopicModelMapperSelector extends MapperSelector[MultiTopicModel, MultiTopicDBModel]

object MultiTopicModelMapperV1 extends SimpleMapper[MultiTopicModel, MultiTopicDBModelV1] {
  override val version = "multiTopicV1"
  override def fromDBModelToModel[B >: MultiTopicDBModelV1](m: B): MultiTopicModel = m match {
    case mm: MultiTopicDBModelV1 => transform[MultiTopicModel](mm)
    case o                     => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")
  }
}
