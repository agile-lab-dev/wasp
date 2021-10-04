package it.agilelab.bigdata.wasp.repository.core.mappers

import it.agilelab.bigdata.wasp.models.CdcModel
import it.agilelab.bigdata.wasp.repository.core.dbModels.{CdcDBModel, CdcDBModelV1}

object CdcMapperSelector extends MapperSelector[CdcModel, CdcDBModel]{

  override def select(model : CdcDBModel) : Mapper[CdcModel, CdcDBModel] = {

    model match {
      case _: CdcDBModelV1 => CdcMapperV1
      case _ => throw new Exception("There is no available mapper for this DBModel, create one!")
    }
  }

  def applyMap(p: CdcDBModel) : CdcModel = {
    val mapper = select(p)
    mapper.fromDBModelToModel(p)
  }
}

object CdcMapperV1 extends Mapper[CdcModel, CdcDBModelV1] {
  override val version = "cdcV1"

  override def fromModelToDBModel(p: CdcModel): CdcDBModelV1 = {

    val values = CdcModel.unapply(p).get
    val makeDBModel = (CdcDBModelV1.apply _).tupled
    makeDBModel(values)
  }


  override def fromDBModelToModel[B >: CdcDBModelV1](p: B): CdcModel = {

    val values = CdcDBModelV1.unapply(p.asInstanceOf[CdcDBModelV1]).get
    val makeProducer = (CdcModel.apply _).tupled
    makeProducer(values)
  }

}