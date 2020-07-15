package it.agile.bigdata.wasp.repository.postgres.tables

import it.agilelab.bigdata.wasp.models.ProducerModel
import it.agilelab.bigdata.wasp.utils.JsonSupport
import spray.json.JsValue
import spray.json._

object ProducerTableDefinition extends ModelTableDefinition[ProducerModel] with JsonSupport {

  override def tableName: String = "PRODUCER"

  val isActive : String = "is_active"
  val isSystem : String = "is_system"
  val topicName : String = "topic_name"


  override def columns: List[String] = super.columns ::: List(isActive,isSystem,topicName)



  override def ddl : String = s"""CREATE TABLE IF NOT EXISTS $tableName (
                        |$name varchar NOT NULL,
                        |$topicName varchar,
                        |$isActive boolean,
                        |$isSystem boolean,
                        |$payload json,
                        |PRIMARY KEY ($name))
                        |""".stripMargin

  override def fromJsonToModel(json: JsValue): ProducerModel = json.convertTo[ProducerModel]

  override def fromModelToJson(model: ProducerModel): JsValue = model.toJson


  override protected def extraColumns: List[String] = List(topicName,isActive,isSystem)


  override protected def mapperExtraColumnsFromModelToArray: ProducerModel => Array[(String, Any)] = model=> Array(
    (topicName,model.topicName.orNull),
    (isActive,model.isActive),
    (isSystem,model.isSystem)
  )






}
