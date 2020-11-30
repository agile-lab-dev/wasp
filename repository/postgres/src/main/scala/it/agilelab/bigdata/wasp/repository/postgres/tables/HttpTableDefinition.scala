package it.agilelab.bigdata.wasp.repository.postgres.tables;

import it.agilelab.bigdata.wasp.models.{HttpModel}
import it.agilelab.bigdata.wasp.utils.JsonSupport
import spray.json._

object HttpTableDefinition extends SimpleModelTableDefinition[HttpModel] with JsonSupport {
  override protected def fromModelToJson(model: HttpModel): JsValue = model.toJson

  override protected def fromJsonToModel(json: JsValue): HttpModel = json.convertTo[HttpModel]

  override def tableName: String = "HTTP"
}
