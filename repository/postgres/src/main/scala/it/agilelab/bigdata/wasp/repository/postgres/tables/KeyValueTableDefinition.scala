package it.agilelab.bigdata.wasp.repository.postgres.tables

import it.agilelab.bigdata.wasp.models.KeyValueModel
import it.agilelab.bigdata.wasp.utils.JsonSupport
import spray.json._

object KeyValueTableDefinition extends SimpleModelTableDefinition[KeyValueModel] with JsonSupport{

  val tableName = "KEY_VALUE"

  override def fromJsonToModel(json: JsValue): KeyValueModel = json.convertTo[KeyValueModel]

  override def fromModelToJson(model: KeyValueModel): JsValue = model.toJson

}