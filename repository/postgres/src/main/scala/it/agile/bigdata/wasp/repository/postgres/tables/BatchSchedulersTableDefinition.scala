package it.agile.bigdata.wasp.repository.postgres.tables
import it.agilelab.bigdata.wasp.models.BatchSchedulerModel
import it.agilelab.bigdata.wasp.utils.JsonSupport
import spray.json._

object BatchSchedulersTableDefinition extends ModelTableDefinition[BatchSchedulerModel] with JsonSupport{

  val isActive = "is_active"

  override def tableName: String = "BATCH_SCHEDULERS"

  override  def ddl : String = s"""CREATE TABLE IF NOT EXISTS $tableName (
                                  |$name varchar NOT NULL,
                                  |$isActive boolean,
                                  |$payload json,
                                  |PRIMARY KEY ($name))
                                  |""".stripMargin

  override protected def extraColumns: List[String] = List(isActive)

  override protected def mapperExtraColumnsFromModelToArray: BatchSchedulerModel => Array[(String, Any)] = m => Array(
    (isActive,m.isActive)
  )

  override protected def fromModelToJson(model: BatchSchedulerModel): JsValue = model.toJson

  override protected def fromJsonToModel(json: JsValue): BatchSchedulerModel = json.convertTo[BatchSchedulerModel]


}
