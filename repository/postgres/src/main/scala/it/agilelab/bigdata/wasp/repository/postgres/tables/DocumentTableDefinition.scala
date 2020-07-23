package it.agilelab.bigdata.wasp.repository.postgres.tables

import it.agilelab.bigdata.wasp.models.DocumentModel
import it.agilelab.bigdata.wasp.utils.JsonSupport
import spray.json._


object DocumentTableDefinition extends SimpleModelTableDefinition[DocumentModel] with JsonSupport{

  override def tableName: String = "DOCUMENT"

  override protected def fromModelToJson(model: DocumentModel): JsValue = model.toJson

  override protected def fromJsonToModel(json: JsValue): DocumentModel =  json.convertTo[DocumentModel]


}
