package it.agile.bigdata.wasp.repository.postgres.tables

import java.sql.ResultSet

import it.agilelab.bigdata.wasp.models.PipegraphInstanceModel
import it.agilelab.bigdata.wasp.models.PipegraphStatus.PipegraphStatus
import it.agilelab.bigdata.wasp.utils.JsonSupport
import spray.json._

object PipegraphInstanceTableDefinition extends ModelTableDefinition[PipegraphInstanceModel] with JsonSupport {

  val instanceOf =  "instance_of"
  private val startTimestamp = "start_timestamp"
  private val currentStatusTimestamp = "current_status_timestamp"
  private val status = "status"
  private val error = "error"






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
