package it.agilelab.bigdata.wasp.repository.core.mappers

import it.agilelab.bigdata.wasp.models.RawModel
import it.agilelab.bigdata.wasp.repository.core.dbModels.{RawDBModel, RawDBModelV1}

object RawMapperSelector extends MapperSelector[RawModel, RawDBModel]{

  override def select(model : RawDBModel) : Mapper[RawModel, RawDBModel] = {

    model match {
      case _: RawDBModelV1 => RawMapperV1
      case _ => throw new Exception("There is no available mapper for this DBModel, create one!")
    }
  }

  def applyMap(p: RawDBModel) : RawModel = {
    val mapper = select(p)
    mapper.fromDBModelToModel(p)
  }
}

object RawMapperV1 extends Mapper[RawModel, RawDBModelV1] {
  override val version = "rawV1"

  override def fromModelToDBModel(p: RawModel): RawDBModelV1 = {

    val values = RawModel.unapply(p).get
    val makeDBModel = (RawDBModelV1.apply _).tupled
    makeDBModel(values)
  }


  override def fromDBModelToModel[B >: RawDBModelV1](p: B): RawModel = {

    val values = RawDBModelV1.unapply(p.asInstanceOf[RawDBModelV1]).get
    val makeProducer = (RawModel.apply _).tupled
    makeProducer(values)
  }
}