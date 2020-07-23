package it.agilelab.bigdata.wasp.repository.postgres.tables

import it.agilelab.bigdata.wasp.models.RawModel
import it.agilelab.bigdata.wasp.utils.JsonSupport
import spray.json._

object RawTableDefinition extends SimpleModelTableDefinition[RawModel] with JsonSupport {

  override def tableName: String = "RAW"

  override def fromJsonToModel(json: JsValue): RawModel =
    json.convertTo[RawModel]

  override def fromModelToJson(model: RawModel): JsValue = model.toJson

}
