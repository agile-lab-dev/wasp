package it.agilelab.bigdata.wasp.repository.core.mappers

import it.agilelab.bigdata.wasp.models.IndexModel
import it.agilelab.bigdata.wasp.repository.core.dbModels.{CdcDBModelV1, IndexDBModel, IndexDBModelV1}

object IndexDBModelMapperSelector extends MapperSelector[IndexModel, IndexDBModel] {

  override def select(model: IndexDBModel): Mapper[IndexModel, IndexDBModel] = {

    model match {
      case _: IndexDBModelV1 => IndexMapperV1
      case o                 => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")
    }
  }

  def applyMap(p: IndexDBModel): IndexModel = {
    val mapper = select(p)
    mapper.fromDBModelToModel(p)
  }
}

object IndexMapperV1 extends Mapper[IndexModel, IndexDBModelV1] {
  override val version = "indexV1"

  override def fromModelToDBModel(p: IndexModel): IndexDBModelV1 = {

    val values      = IndexModel.unapply(p).get
    val makeDBModel = (IndexDBModelV1.apply _).tupled
    makeDBModel(values)
  }

  override def fromDBModelToModel[B >: IndexDBModelV1](p: B): IndexModel = {

    val values       = IndexDBModelV1.unapply(p.asInstanceOf[IndexDBModelV1]).get
    val makeProducer = (IndexModel.apply _).tupled
    makeProducer(values)
  }
}
