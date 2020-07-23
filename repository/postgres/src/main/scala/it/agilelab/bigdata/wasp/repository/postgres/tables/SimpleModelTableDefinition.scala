package it.agilelab.bigdata.wasp.repository.postgres.tables

import it.agilelab.bigdata.wasp.models.Model
import spray.json.{JsValue, _}



trait SimpleModelTableDefinition[T<:Model] extends ModelTableDefinition[T] {

  def ddl : String = s"""CREATE TABLE IF NOT EXISTS $tableName (
               |$name varchar NOT NULL,
               |$payload json,
               |PRIMARY KEY ($name))
               |""".stripMargin

  override protected def extraColumns: List[String] = List.empty

  override protected def mapperExtraColumnsFromModelToArray: T => Array[(String, Any)] = _ => Array.empty

  override protected def fromModelToJson(model: T): JsValue

  override protected def fromJsonToModel(json: JsValue): T

  override def tableName: String

}
