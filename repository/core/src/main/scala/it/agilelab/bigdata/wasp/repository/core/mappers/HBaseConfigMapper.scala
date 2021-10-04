package it.agilelab.bigdata.wasp.repository.core.mappers

import it.agilelab.bigdata.wasp.models.configuration.HBaseConfigModel
import it.agilelab.bigdata.wasp.repository.core.dbModels.{HBaseConfigDBModel, HBaseConfigDBModelV1}

object HBaseConfigMapperSelector extends MapperSelector[HBaseConfigModel, HBaseConfigDBModel] {
  override def select(model: HBaseConfigDBModel): Mapper[HBaseConfigModel, HBaseConfigDBModel] = {

    model match {
      case _: HBaseConfigDBModelV1 => HBaseConfigMapperV1
      case _ => throw new Exception("There is no available mapper for this DBModel, create one!")
    }
  }

  def applyMap(p: HBaseConfigDBModel): HBaseConfigModel = {
    val mapper = select(p)
    mapper.fromDBModelToModel(p)
  }
}

object HBaseConfigMapperV1 extends Mapper[HBaseConfigModel, HBaseConfigDBModelV1] {
  override val version = "hbaseConfigV1"

  override def fromModelToDBModel(p: HBaseConfigModel): HBaseConfigDBModelV1 = {

    val values = HBaseConfigModel.unapply(p).get
    val makeDBModel = (HBaseConfigDBModelV1.apply _).tupled
    makeDBModel(values)
  }


  override def fromDBModelToModel[B >: HBaseConfigDBModelV1](p: B): HBaseConfigModel = {

    val values = HBaseConfigDBModelV1.unapply(p.asInstanceOf[HBaseConfigDBModelV1]).get
    val makeProducer = (HBaseConfigModel.apply _).tupled
    makeProducer(values)
  }
}