package it.agile.bigdata.wasp.repository.postgres.tables

import it.agilelab.bigdata.wasp.models.ProcessGroupModel
import it.agilelab.bigdata.wasp.utils.JsonSupport
import spray.json._

object ProcessGroupTableDefinition extends SimpleModelTableDefinition[ProcessGroupModel] with JsonSupport{

  override def tableName: String = "PROCESS_GROUP"

  // uses BsonConvertToSprayJson for the content
  override protected def fromModelToJson(model: ProcessGroupModel): JsValue = model.toJson

  override protected def fromJsonToModel(json: JsValue): ProcessGroupModel = json.convertTo[ProcessGroupModel]

}
