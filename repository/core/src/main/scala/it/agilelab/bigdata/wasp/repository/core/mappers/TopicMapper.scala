package it.agilelab.bigdata.wasp.repository.core.mappers

import it.agilelab.bigdata.wasp.models.{DatastoreModel, TopicModel}
import it.agilelab.bigdata.wasp.repository.core.dbModels.{MultiTopicDBModelV1, TopicDBModel, TopicDBModelV1}

object TopicDBModelMapperSelector extends MapperSelector[TopicModel, TopicDBModel] {

  override def select(model: TopicDBModel): Mapper[TopicModel, TopicDBModel] = {

    model match {
      case _: TopicDBModelV1 => TopicMapperV1
      case o                 => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")
    }
  }

  def applyMap(p: TopicDBModel): DatastoreModel = {
    val mapper = select(p)
    mapper.fromDBModelToModel(p)
  }
}

object TopicMapperV1 extends SimpleMapper[TopicModel, TopicDBModelV1] {
  override val version = "topicV1"
  override def fromDBModelToModel[B >: TopicDBModelV1](m: B): TopicModel = m match {
    case mm: TopicDBModelV1 => transform[TopicModel](mm)
    case o                     => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")
  }}
