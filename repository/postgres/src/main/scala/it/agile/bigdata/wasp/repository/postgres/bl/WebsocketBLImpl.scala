package it.agile.bigdata.wasp.repository.postgres.bl

import it.agile.bigdata.wasp.repository.postgres.WaspPostgresDB
import it.agile.bigdata.wasp.repository.postgres.tables.{TableDefinition, WebSocketTableDefinition}
import it.agilelab.bigdata.wasp.models.WebsocketModel
import it.agilelab.bigdata.wasp.repository.core.bl.WebsocketBL

case class WebsocketBLImpl(waspDB : WaspPostgresDB) extends WebsocketBL with PostgresBL  {

  implicit val tableDefinition: TableDefinition[WebsocketModel,String] = WebSocketTableDefinition

  override def getByName(name: String): Option[WebsocketModel] = waspDB.getByPrimaryKey(name)

  override def persist(topicModel: WebsocketModel): Unit = waspDB.insert(topicModel)

}
