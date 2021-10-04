package it.agilelab.bigdata.wasp.repository.core.mappers

import it.agilelab.bigdata.wasp.models.{DatastoreModel, MultiTopicModel}
import it.agilelab.bigdata.wasp.repository.core.dbModels.{MultiTopicDBModel, MultiTopicDBModelV1}

object MultiTopicModelMapperSelector extends MapperSelector[MultiTopicModel, MultiTopicDBModel] {
  override def select(model: MultiTopicDBModel): Mapper[MultiTopicModel, MultiTopicDBModel] = {

    model match {
      case _: MultiTopicDBModelV1 => MultiTopicModelMapperV1
      case _ => throw new Exception("There is no available mapper for this DBModel, create one!")
    }
  }

  def applyMap(p: MultiTopicDBModel): DatastoreModel = {
    val mapper = select(p)
    mapper.fromDBModelToModel(p)
  }
}


object MultiTopicModelMapperV1 extends Mapper[MultiTopicModel, MultiTopicDBModelV1] {
  override val version = "multiTopicV1"

  override def fromModelToDBModel(p: MultiTopicModel): MultiTopicDBModelV1 = {

    val values = MultiTopicModel.unapply(p).get
    val makeDBModel = (MultiTopicDBModelV1.apply _).tupled
    makeDBModel(values)
  }


  override def fromDBModelToModel[B >: MultiTopicDBModelV1](p: B): MultiTopicModel = {

    val values = MultiTopicDBModelV1.unapply(p.asInstanceOf[MultiTopicDBModelV1]).get
    val makeProducer = (MultiTopicModel.apply _).tupled
    makeProducer(values)
  }
}