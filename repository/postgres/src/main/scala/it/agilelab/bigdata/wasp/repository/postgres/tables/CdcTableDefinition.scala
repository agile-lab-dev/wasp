package it.agilelab.bigdata.wasp.repository.postgres.tables

import it.agilelab.bigdata.wasp.models.CdcModel
import it.agilelab.bigdata.wasp.utils.JsonSupport
import spray.json._

object CdcTableDefinition extends SimpleModelTableDefinition[CdcModel] with JsonSupport {

  override def tableName: String = "CDC"

  override def fromJsonToModel(json: JsValue): CdcModel =
    json.convertTo[CdcModel]

  override def fromModelToJson(model: CdcModel): JsValue = model.toJson

}
