package it.agilelab.bigdata.wasp.repository.postgres.tables

import it.agilelab.bigdata.wasp.models.IndexModel
import it.agilelab.bigdata.wasp.utils.JsonSupport
import spray.json.JsValue
import spray.json._



object IndexTableDefinition extends SimpleModelTableDefinition[IndexModel] with JsonSupport{
  override protected def fromModelToJson(model: IndexModel): JsValue = model.toJson

  override protected def fromJsonToModel(json: JsValue): IndexModel = json.convertTo[IndexModel]

  override def tableName: String = "INDEX"
}
