package it.agilelab.bigdata.wasp.repository.core.mappers

import it.agilelab.bigdata.wasp.models.FreeCodeModel
import it.agilelab.bigdata.wasp.repository.core.dbModels.{FreeCodeDBModel, FreeCodeDBModelV1}

object FreeCodeMapperSelector extends MapperSelector[FreeCodeModel, FreeCodeDBModel] {

  override def select(model: FreeCodeDBModel): Mapper[FreeCodeModel, FreeCodeDBModel] = {

    model match {
      case _: FreeCodeDBModelV1 => FreeCodeMapperV1
      case _ => throw new Exception("There is no available mapper for this DBModel, create one!")
    }
  }

  def applyMap(p: FreeCodeDBModel): FreeCodeModel = {
    val mapper = select(p)
    mapper.fromDBModelToModel(p)
  }
}

object FreeCodeMapperV1 extends Mapper[FreeCodeModel, FreeCodeDBModelV1] {
  override val version = "freeCodeV1"

  override def fromModelToDBModel(p: FreeCodeModel): FreeCodeDBModelV1 = {

    val values = FreeCodeModel.unapply(p).get
    val makeDBModel = (FreeCodeDBModelV1.apply _).tupled
    makeDBModel(values)
  }


  override def fromDBModelToModel[B >: FreeCodeDBModelV1](p: B): FreeCodeModel = {

    val values = FreeCodeDBModelV1.unapply(p.asInstanceOf[FreeCodeDBModelV1]).get
    val makeProducer = (FreeCodeModel.apply _).tupled
    makeProducer(values)
  }

}

