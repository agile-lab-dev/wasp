package it.agilelab.bigdata.wasp.repository.postgres.tables

import it.agilelab.bigdata.wasp.models.BatchJobInstanceModel
import it.agilelab.bigdata.wasp.utils.JsonSupport
import spray.json._

object BatchJobInstanceTableDefinition extends ModelTableDefinition[BatchJobInstanceModel] with JsonSupport {

  val instanceOf = "instance_of"

  override protected def extraColumns: List[String] = List(instanceOf)

  override protected def mapperExtraColumnsFromModelToArray: BatchJobInstanceModel => Array[(String, Any)] = m=> Array(
    (instanceOf,m.instanceOf)
  )


  def ddl : String = s"""CREATE TABLE IF NOT EXISTS $tableName (
                        |$name varchar NOT NULL,
                        |$instanceOf varchar,
                        |$payload json,
                        |PRIMARY KEY ($name))
                        |""".stripMargin

  override protected def fromModelToJson(model: BatchJobInstanceModel): JsValue = model.toJson

  override protected def fromJsonToModel(json: JsValue): BatchJobInstanceModel = json.convertTo[BatchJobInstanceModel]

  override def tableName: String = "BATCH_JOB_INSTANCE"
}
