package it.agilelab.bigdata.wasp.repository.postgres.tables

import it.agilelab.bigdata.wasp.models.WebsocketModel
import it.agilelab.bigdata.wasp.utils.JsonSupport
import spray.json.JsValue
import spray.json._

object WebSocketTableDefinition extends SimpleModelTableDefinition[WebsocketModel] with JsonSupport{

  override protected def fromModelToJson(model: WebsocketModel): JsValue = model.toJson

  override protected def fromJsonToModel(json: JsValue): WebsocketModel = json.convertTo[WebsocketModel]

  override def tableName: String = "WEB_SOCKET"

}
