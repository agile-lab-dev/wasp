package it.agile.bigdata.wasp.repository.postgres.tables

import java.sql.ResultSet

import it.agilelab.bigdata.wasp.models.Model
import spray.json.{JsValue,_}


trait ModelTableDefinition[T<:Model] extends TableDefinition[T,String]{

  protected val name = "name"
  protected val payload = "payload"

  protected def extraColumns : List[String]
  def columns: List[String] = List(name, payload) ::: extraColumns

  override val from: ResultSet => T = rs => fromJsonToModel(rs.getString(payload).parseJson)

  override def to: T => Array[(String, Any)] = model=> Array(
    (name,model.name),
    (payload,fromModelToJson(model).toString())
  ) ++ mapperExtraColumnsFromModelToArray(model)


  protected def mapperExtraColumnsFromModelToArray: T => Array[(String, Any)]

  protected def fromModelToJson(model : T) : JsValue
  protected def fromJsonToModel(json : JsValue) : T

  override lazy val conditionPrimaryKey : String => String = k=> s"$name='$k'"
  override lazy val primaryKeyFromObject : T => String = obj => obj.name


}
