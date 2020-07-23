package it.agilelab.bigdata.wasp.repository.postgres.tables

import it.agilelab.bigdata.wasp.datastores.TopicCategory
import it.agilelab.bigdata.wasp.models.DatastoreModel
import it.agilelab.bigdata.wasp.utils.JsonSupport
import spray.json._

object TopicTableDefinition extends SimpleModelTableDefinition[DatastoreModel[TopicCategory]] with JsonSupport{

  override protected def fromModelToJson(model: DatastoreModel[TopicCategory]): JsValue = model.toJson

  override protected def fromJsonToModel(json: JsValue): DatastoreModel[TopicCategory] = json.convertTo[DatastoreModel[TopicCategory]]

  override def tableName: String = "TOPIC"

}
