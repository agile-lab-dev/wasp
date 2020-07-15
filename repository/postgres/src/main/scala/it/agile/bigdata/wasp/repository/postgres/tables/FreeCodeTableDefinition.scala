package it.agile.bigdata.wasp.repository.postgres.tables

import it.agilelab.bigdata.wasp.models.FreeCodeModel
import spray.json.JsValue
import it.agilelab.bigdata.wasp.utils.JsonSupport
import spray.json._


object FreeCodeTableDefinition extends SimpleModelTableDefinition[FreeCodeModel] with JsonSupport{

  val tableName = "FREE_CODE"

  override def fromJsonToModel(json: JsValue): FreeCodeModel = json.convertTo[FreeCodeModel]

  override def fromModelToJson(model: FreeCodeModel): JsValue = model.toJson

}
