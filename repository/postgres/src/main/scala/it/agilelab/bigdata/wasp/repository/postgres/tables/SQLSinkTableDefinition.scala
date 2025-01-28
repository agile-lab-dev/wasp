package it.agilelab.bigdata.wasp.repository.postgres.tables

import it.agilelab.bigdata.wasp.models.SQLSinkModel
import it.agilelab.bigdata.wasp.utils.JsonSupport
import spray.json._

object SQLSinkTableDefinition extends SimpleModelTableDefinition[SQLSinkModel] with JsonSupport {

  override def tableName: String = "SQLSINK"

  override protected def fromModelToJson(model: SQLSinkModel): JsValue = model.toJson

  override protected def fromJsonToModel(json: JsValue): SQLSinkModel = json.convertTo[SQLSinkModel]

}
