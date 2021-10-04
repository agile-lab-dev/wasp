package it.agilelab.bigdata.wasp.repository.core.mappers

import it.agilelab.bigdata.wasp.models.{PipegraphInstanceModel, PipegraphModel}
import it.agilelab.bigdata.wasp.repository.core.dbModels.{PipegraphDBModel, PipegraphDBModelV1, PipegraphInstanceDBModel, PipegraphInstanceDBModelV1}



object PipegraphDBModelMapperSelector extends MapperSelector[PipegraphModel, PipegraphDBModel]{


  override def select(model : PipegraphDBModel) : Mapper[PipegraphModel, PipegraphDBModel] = {

    model match {
      case _: PipegraphDBModelV1 => PipegraphMapperV1
      case _ => throw new Exception("There is no available mapper for this DBModel, create one!")
    }
  }

  def applyMap(p: PipegraphDBModel) : PipegraphModel = {
    val mapper = select(p)
    mapper.fromDBModelToModel(p)
  }
}

object PipegraphInstanceDBModelMapperSelector extends MapperSelector[PipegraphInstanceModel, PipegraphInstanceDBModel]{


  override def select(model : PipegraphInstanceDBModel) : Mapper[PipegraphInstanceModel, PipegraphInstanceDBModel] = {

    model match {
      case _: PipegraphInstanceDBModelV1 => PipegraphInstanceMapperV1
      case _ => throw new Exception("There is no available mapper for this DBModel, create one!")
    }
  }

  def applyMap(p: PipegraphInstanceDBModel) : PipegraphInstanceModel = {
    val mapper = select(p)
    mapper.fromDBModelToModel(p)
  }
}



object PipegraphMapperV1 extends Mapper[PipegraphModel, PipegraphDBModelV1] {
  override val version = "pipegraphV1"

  override def fromModelToDBModel(p: PipegraphModel): PipegraphDBModelV1 = {

    val values = PipegraphModel.unapply(p).get
    val makeDBModel = (PipegraphDBModelV1.apply _).tupled
    makeDBModel(values)
  }


  override def fromDBModelToModel[B >: PipegraphDBModelV1](p: B): PipegraphModel = {

    val values = PipegraphDBModelV1.unapply(p.asInstanceOf[PipegraphDBModelV1]).get
    val makeProducer = (PipegraphModel.apply _).tupled
    makeProducer(values)
  }
}

object PipegraphInstanceMapperV1 extends Mapper[PipegraphInstanceModel, PipegraphInstanceDBModelV1] {
  override val version = "pipegraphInstanceV1"

  override def fromModelToDBModel(p: PipegraphInstanceModel): PipegraphInstanceDBModelV1 = {

    val values = PipegraphInstanceModel.unapply(p).get
    val makeDBModel = (PipegraphInstanceDBModelV1.apply _).tupled
    makeDBModel(values)
  }


  override def fromDBModelToModel[B >: PipegraphInstanceDBModelV1](p: B): PipegraphInstanceModel = {

    val values = PipegraphInstanceDBModelV1.unapply(p.asInstanceOf[PipegraphInstanceDBModelV1]).get
    val makeProducer = (PipegraphInstanceModel.apply _).tupled
    makeProducer(values)
  }
}
