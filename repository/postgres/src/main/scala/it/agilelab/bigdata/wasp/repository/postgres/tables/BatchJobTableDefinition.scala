package it.agilelab.bigdata.wasp.repository.postgres.tables

import it.agilelab.bigdata.wasp.models.BatchJobModel
import it.agilelab.bigdata.wasp.utils.JsonSupport
import spray.json._

object BatchJobTableDefinition extends SimpleModelTableDefinition[BatchJobModel] with JsonSupport{

  val tableName = "BATCH_JOB"

  override def fromJsonToModel(json: JsValue): BatchJobModel = json.convertTo[BatchJobModel]

  override def fromModelToJson(model: BatchJobModel): JsValue = model.toJson


}
