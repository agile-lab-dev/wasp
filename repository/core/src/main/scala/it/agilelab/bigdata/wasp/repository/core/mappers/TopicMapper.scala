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

object TopicMapperV1 extends Mapper[TopicModel, TopicDBModelV1] {
  override val version = "topicV1"

  override def fromModelToDBModel(p: TopicModel): TopicDBModelV1 = {

    val values      = TopicModel.unapply(p).get
    val makeDBModel = (TopicDBModelV1.apply _).tupled
    makeDBModel(values)
  }

  override def fromDBModelToModel[B >: TopicDBModelV1](p: B): TopicModel = {

    val values       = TopicDBModelV1.unapply(p.asInstanceOf[TopicDBModelV1]).get
    val makeProducer = (TopicModel.apply _).tupled
    makeProducer(values)
  }
}
