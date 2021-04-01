package it.agilelab.bigdata.wasp.repository.postgres.tables

import it.agilelab.bigdata.wasp.models.{GenericModel}
import it.agilelab.bigdata.wasp.utils.JsonSupport
import spray.json._

object GenericTableDefinition extends SimpleModelTableDefinition[GenericModel] with JsonSupport {

  override def tableName: String = "GENERIC"

  override def fromJsonToModel(json: JsValue): GenericModel =
    json.convertTo[GenericModel]

  override def fromModelToJson(model: GenericModel): JsValue = model.toJson

}
