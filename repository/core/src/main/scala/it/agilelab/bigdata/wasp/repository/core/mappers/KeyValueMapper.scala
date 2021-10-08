package it.agilelab.bigdata.wasp.repository.core.mappers

import it.agilelab.bigdata.wasp.models.KeyValueModel
import it.agilelab.bigdata.wasp.repository.core.dbModels.{IndexDBModelV1, KeyValueDBModel, KeyValueDBModelV1}

object KeyValueMapperSelector extends MapperSelector[KeyValueModel, KeyValueDBModel] {
  override def select(model: KeyValueDBModel): Mapper[KeyValueModel, KeyValueDBModel] = {

    model match {
      case _: KeyValueDBModelV1 => KeyValueMapperV1
      case o                    => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")
    }
  }

  def applyMap(p: KeyValueDBModel): KeyValueModel = {
    val mapper = select(p)
    mapper.fromDBModelToModel(p)
  }
}

object KeyValueMapperV1 extends Mapper[KeyValueModel, KeyValueDBModelV1] {
  override val version = "keyValueV1"

  override def fromModelToDBModel(p: KeyValueModel): KeyValueDBModelV1 = {

    val values      = KeyValueModel.unapply(p).get
    val makeDBModel = (KeyValueDBModelV1.apply _).tupled
    makeDBModel(values)
  }

  override def fromDBModelToModel[B >: KeyValueDBModelV1](p: B): KeyValueModel = {

    val values       = KeyValueDBModelV1.unapply(p.asInstanceOf[KeyValueDBModelV1]).get
    val makeProducer = (KeyValueModel.apply _).tupled
    makeProducer(values)
  }
}
