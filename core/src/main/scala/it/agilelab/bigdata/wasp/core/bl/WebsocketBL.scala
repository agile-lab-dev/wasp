package it.agilelab.bigdata.wasp.core.bl

import it.agilelab.bigdata.wasp.core.models.WebsocketModel

trait WebsocketBL {
  def getByName(name: String): Option[WebsocketModel]

  def persist(topicModel: WebsocketModel): Unit
}