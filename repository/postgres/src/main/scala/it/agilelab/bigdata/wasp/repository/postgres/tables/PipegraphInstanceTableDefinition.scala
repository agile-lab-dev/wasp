package it.agilelab.bigdata.wasp.repository.postgres.tables

import it.agilelab.bigdata.wasp.models.PipegraphInstanceModel
import it.agilelab.bigdata.wasp.utils.JsonSupport
import spray.json._

object PipegraphInstanceTableDefinition extends ModelTableDefinition[PipegraphInstanceModel] with JsonSupport {

  val instanceOf =  "instance_of"

  override val tableName: String = "PIPEGRAPH_INSTANCE"


  override protected def extraColumns: List[String] = List(instanceOf)

  override protected def mapperExtraColumnsFromModelToArray: PipegraphInstanceModel => Array[(String, Any)] = m => Array(
    (instanceOf,m.instanceOf)
  )

  override protected def fromModelToJson(model: PipegraphInstanceModel): JsValue = model.toJson

  override protected def fromJsonToModel(json: JsValue): PipegraphInstanceModel = json.convertTo[PipegraphInstanceModel]

  override val ddl: String =  s"""CREATE TABLE IF NOT EXISTS $tableName (
                                  |$name varchar NOT NULL,
                                  |$instanceOf varchar,
                                  |$payload json,
                                  |PRIMARY KEY ($name))
                                  |""".stripMargin


}
