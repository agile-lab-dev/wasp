package it.agilelab.bigdata.wasp.repository.core.mappers

import it.agilelab.bigdata.wasp.models.MlModelOnlyInfo
import it.agilelab.bigdata.wasp.repository.core.dbModels.{MlDBModelOnlyInfo, MlDBModelOnlyInfoV1}

object MlDBModelMapperSelector extends MapperSelector[MlModelOnlyInfo, MlDBModelOnlyInfo]{

  override def select(model : MlDBModelOnlyInfo) : Mapper[MlModelOnlyInfo, MlDBModelOnlyInfo] = {

    model match {
      case _: MlDBModelOnlyInfoV1 => MlDBModelMapperV1
      case _ => throw new Exception("There is no available mapper for this DBModel, create one!")
    }
  }

  def applyMap(p: MlDBModelOnlyInfo) : MlModelOnlyInfo = {
    val mapper = select(p)
    mapper.fromDBModelToModel(p)
  }
}

object MlDBModelMapperV1 extends Mapper[MlModelOnlyInfo, MlDBModelOnlyInfoV1] {
  override val version = "mlModelV1"

  override def fromModelToDBModel(p: MlModelOnlyInfo): MlDBModelOnlyInfoV1 = {

    val values = MlModelOnlyInfo.unapply(p).get
    val makeDBModel = (MlDBModelOnlyInfoV1.apply _).tupled
    makeDBModel(values)
  }


  override def fromDBModelToModel[B >: MlDBModelOnlyInfoV1](p: B): MlModelOnlyInfo = {

    val values = MlDBModelOnlyInfoV1.unapply(p.asInstanceOf[MlDBModelOnlyInfoV1]).get
    val makeProducer = (MlModelOnlyInfo.apply _).tupled
    makeProducer(values)
  }
}
