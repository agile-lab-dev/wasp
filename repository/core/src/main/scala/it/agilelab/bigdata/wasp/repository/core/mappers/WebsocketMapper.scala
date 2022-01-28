package it.agilelab.bigdata.wasp.repository.core.mappers

import it.agilelab.bigdata.wasp.models.WebsocketModel
import it.agilelab.bigdata.wasp.repository.core.dbModels.{WebsocketDBModel, WebsocketDBModelV1}

object WebsocketMapperSelector extends MapperSelector[WebsocketModel, WebsocketDBModel]

object WebsocketMapperV1 extends SimpleMapper[WebsocketModel, WebsocketDBModelV1] {
  override val version = "websocketV1"
  override def fromDBModelToModel[B >: WebsocketDBModelV1](m: B): WebsocketModel = m match {
    case mm: WebsocketDBModelV1 => transform[WebsocketModel](mm)
    case o                     => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")
  }}
