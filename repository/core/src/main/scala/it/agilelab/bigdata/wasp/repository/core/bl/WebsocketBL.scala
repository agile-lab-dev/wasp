package it.agilelab.bigdata.wasp.repository.core.bl

import it.agilelab.bigdata.wasp.models.WebsocketModel

trait WebsocketBL {
  def getByName(name: String): Option[WebsocketModel]

  def persist(topicModel: WebsocketModel): Unit
}