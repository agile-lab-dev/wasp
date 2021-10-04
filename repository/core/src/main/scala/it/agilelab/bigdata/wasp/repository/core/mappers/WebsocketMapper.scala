package it.agilelab.bigdata.wasp.repository.core.mappers

import it.agilelab.bigdata.wasp.models.WebsocketModel
import it.agilelab.bigdata.wasp.repository.core.dbModels.{WebsocketDBModel, WebsocketDBModelV1}

object WebsocketMapperSelector extends MapperSelector[WebsocketModel, WebsocketDBModel]{

  override def select(model : WebsocketDBModel) : Mapper[WebsocketModel, WebsocketDBModel] = {

    model match {
      case _: WebsocketDBModelV1 => WebsocketMapperV1
      case _ => throw new Exception("There is no available mapper for this DBModel, create one!")
    }
  }

  def applyMap(p: WebsocketDBModel) : WebsocketModel = {
    val mapper = select(p)
    mapper.fromDBModelToModel(p)
  }
}

object WebsocketMapperV1 extends Mapper[WebsocketModel, WebsocketDBModelV1] {
  override val version = "websocketV1"

  override def fromModelToDBModel(p: WebsocketModel): WebsocketDBModelV1 = {

    val values = WebsocketModel.unapply(p).get
    val makeDBModel = (WebsocketDBModelV1.apply _).tupled
    makeDBModel(values)
  }

  override def fromDBModelToModel[B >: WebsocketDBModelV1](p: B): WebsocketModel = {

    val values = WebsocketDBModelV1.unapply(p.asInstanceOf[WebsocketDBModelV1]).get
    val makeProducer = (WebsocketModel.apply _).tupled
    makeProducer(values)
  }
}