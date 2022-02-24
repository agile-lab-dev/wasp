package it.agilelab.bigdata.wasp.repository.postgres.tables

import it.agilelab.bigdata.wasp.models.PipegraphModel
import it.agilelab.bigdata.wasp.utils.JsonSupport
import spray.json._

object PipegraphTableDefinition extends ModelTableDefinition[PipegraphModel] with JsonSupport {

  val tableName = "PIPEGRAPH"

  val isSystem = "is_system"
  val owner = "owner"

  override protected def extraColumns: List[String] = List(isSystem)

  override protected def mapperExtraColumnsFromModelToArray: PipegraphModel => Array[(String, Any)] = model => Array(
    (isSystem,model.isSystem)
  )

  override protected def fromModelToJson(model: PipegraphModel): JsValue = model.toJson

  override protected def fromJsonToModel(json: JsValue): PipegraphModel = json.convertTo[PipegraphModel]


  val ddl: String =
    s"""CREATE TABLE IF NOT EXISTS $tableName (
       |$name varchar NOT NULL,
       |$owner varchar,
       |$isSystem boolean,
       |$payload json,
       |PRIMARY KEY ($name))
       |""".stripMargin


}

