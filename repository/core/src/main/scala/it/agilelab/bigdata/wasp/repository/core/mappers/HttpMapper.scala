package it.agilelab.bigdata.wasp.repository.core.mappers

import it.agilelab.bigdata.wasp.models.HttpModel
import it.agilelab.bigdata.wasp.repository.core.dbModels.{CdcDBModelV1, HttpDBModel, HttpDBModelV1}

object HttpDBModelMapperSelector extends MapperSelector[HttpModel, HttpDBModel]{

  override def select(model : HttpDBModel) : Mapper[HttpModel, HttpDBModel] = {

    model match {
      case _: HttpDBModelV1 => HttpMapperV1
      case _ => throw new Exception("There is no available mapper for this DBModel, create one!")
    }
  }

  def applyMap(p: HttpDBModel) : HttpModel = {
    val mapper = select(p)
    mapper.fromDBModelToModel(p)
  }
}

object HttpMapperV1 extends Mapper[HttpModel, HttpDBModelV1] {
  override val version = "httpV1"

  override def fromModelToDBModel(p: HttpModel): HttpDBModelV1 = {

    val values = HttpModel.unapply(p).get
    val makeDBModel = (HttpDBModelV1.apply _).tupled
    makeDBModel(values)
  }


  override def fromDBModelToModel[B >: HttpDBModelV1](p: B): HttpModel = {

    val values = HttpDBModelV1.unapply(p.asInstanceOf[HttpDBModelV1]).get
    val makeProducer = (HttpModel.apply _).tupled
    makeProducer(values)
  }
}
