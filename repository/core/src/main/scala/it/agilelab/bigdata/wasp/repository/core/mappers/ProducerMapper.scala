package it.agilelab.bigdata.wasp.repository.core.mappers

import it.agilelab.bigdata.wasp.models.ProducerModel
import it.agilelab.bigdata.wasp.repository.core.dbModels.{ProducerDBModel, ProducerDBModelV1, ProducerDBModelV2}

object ProducerDBModelMapperSelector extends MapperSelector[ProducerModel, ProducerDBModel]

object ProducerMapperV2 extends Mapper[ProducerModel, ProducerDBModelV2] {
  override val version = "producerV2"

  def fromModelToDBModel(p: ProducerModel): ProducerDBModelV2 = {
    val values      = ProducerModel.unapply(p).get
    val makeDBModel = (ProducerDBModelV2.apply _).tupled
    val t = values match {
      case (
          name: String,
          className: String,
          topicName: Option[String],
          isActive: Boolean,
          configuration: Option[String],
          _,
          _
          ) =>
        (name, className, topicName, isActive, configuration)
    }
    makeDBModel(t)
  }

  override def fromDBModelToModel[B >: ProducerDBModelV2](p: B): ProducerModel = {

    val values       = ProducerDBModelV2.unapply(p.asInstanceOf[ProducerDBModelV2]).get
    val makeProducer = (ProducerModel.apply _).tupled
    val t = values match {
      case (
          name: String,
          className: String,
          topicName: Option[String],
          isActive: Boolean,
          configuration: Option[String]
          ) =>
        (name, className, topicName, isActive, configuration, false, true)
    }
    val producerModel = makeProducer(t)
    producerModel
  }
}

object ProducerMapperV1 extends SimpleMapper[ProducerModel, ProducerDBModelV1] {
  override val version = "producerV1"
  override def fromDBModelToModel[B >: ProducerDBModelV1](m: B): ProducerModel = m match {
    case mm: ProducerDBModelV1 => transform[ProducerModel](mm)
    case o                     => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")
  }
}
