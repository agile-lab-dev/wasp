package it.agilelab.bigdata.wasp.repository.core.mappers

import it.agilelab.bigdata.wasp.models.GenericModel
import it.agilelab.bigdata.wasp.repository.core.dbModels.{GenericDBModel, GenericDBModelV1}

object GenericMapperSelector extends MapperSelector[GenericModel, GenericDBModel] {

  override def select(model: GenericDBModel): Mapper[GenericModel, GenericDBModelV1] = {

    model match {
      case _: GenericDBModelV1 => GenericMapperV1
      case o                   => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")
    }
  }

  def applyMap(p: GenericDBModel): GenericModel = {
    val mapper = select(p)
    mapper.fromDBModelToModel(p)
  }
}

object GenericMapperV1 extends Mapper[GenericModel, GenericDBModelV1] {
  override val version = "rawV1"

  override def fromModelToDBModel(p: GenericModel): GenericDBModelV1 = {

    val values      = GenericModel.unapply(p).get
    val makeDBModel = (GenericDBModelV1.apply _).tupled
    makeDBModel(values)
  }

  override def fromDBModelToModel[B >: GenericDBModelV1](p: B): GenericModel = {

    val values       = GenericDBModelV1.unapply(p.asInstanceOf[GenericDBModelV1]).get
    val makeProducer = (GenericModel.apply _).tupled
    makeProducer(values)
  }
}
