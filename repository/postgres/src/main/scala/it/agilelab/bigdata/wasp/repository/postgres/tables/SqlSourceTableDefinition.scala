package it.agilelab.bigdata.wasp.repository.postgres.tables

import it.agilelab.bigdata.wasp.models.SqlSourceModel
import it.agilelab.bigdata.wasp.utils.JsonSupport
import spray.json._

object SqlSourceTableDefinition extends SimpleModelTableDefinition[SqlSourceModel] with JsonSupport {

  override def tableName: String = "SQL_SOURCE"

  override def fromJsonToModel(json: JsValue): SqlSourceModel =
    json.convertTo[SqlSourceModel]

  override def fromModelToJson(model: SqlSourceModel): JsValue = model.toJson

}
