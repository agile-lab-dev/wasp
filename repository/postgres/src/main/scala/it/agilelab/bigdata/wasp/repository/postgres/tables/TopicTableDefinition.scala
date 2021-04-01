package it.agilelab.bigdata.wasp.repository.postgres.tables

import it.agilelab.bigdata.wasp.models.DatastoreModel
import it.agilelab.bigdata.wasp.utils.JsonSupport
import spray.json._

object TopicTableDefinition extends SimpleModelTableDefinition[DatastoreModel] with JsonSupport{

  override protected def fromModelToJson(model: DatastoreModel): JsValue = model.toJson

  override protected def fromJsonToModel(json: JsValue): DatastoreModel = json.convertTo[DatastoreModel]

  override def tableName: String = "TOPIC"

}
