package it.agilelab.bigdata.wasp.repository.postgres.bl

import it.agilelab.bigdata.wasp.repository.postgres.tables.TableDefinition
import it.agilelab.bigdata.wasp.models.WebsocketModel
import it.agilelab.bigdata.wasp.repository.core.bl.WebsocketBL
import it.agilelab.bigdata.wasp.repository.postgres.WaspPostgresDB
import it.agilelab.bigdata.wasp.repository.postgres.tables.{TableDefinition, WebSocketTableDefinition}

case class WebsocketBLImpl(waspDB : WaspPostgresDB) extends WebsocketBL with PostgresBL  {

  implicit val tableDefinition: TableDefinition[WebsocketModel,String] = WebSocketTableDefinition

  override def getByName(name: String): Option[WebsocketModel] = waspDB.getByPrimaryKey(name)

  override def persist(topicModel: WebsocketModel): Unit = waspDB.insert(topicModel)

}
