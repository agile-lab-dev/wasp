package it.agile.bigdata.wasp.repository.postgres.tables

import java.sql.ResultSet

import it.agilelab.bigdata.wasp.models.{BatchETL, BatchJobExclusionConfig, BatchJobModel}
import it.agilelab.bigdata.wasp.utils.JsonSupport

import scala.reflect.ClassTag
import spray.json._

object BatchJobTableDefinition extends ModelTableDefinition[BatchJobModel] with JsonSupport{




  val tableName = "BATCH_JOB"

  val description = "description"
  val owner = "owner"
  val system = "system"
  val  creationTime = "creationTime"
  val etl = "etl"
  val exclusivityConfig = "exclusivityConfig"

  val columns: Seq[String] = Seq(
    name, description,owner,system,creationTime,etl,exclusivityConfig
  )

  val ddl = s"""CREATE TABLE IF NOT EXISTS $tableName (
               |$name varchar NOT NULL,
               |$description varchar,
               |$owner varchar,
               |$system boolean,
               |$creationTime bigint,
               |$etl varchar,
               |$exclusivityConfig varchar,
               |PRIMARY KEY ($name))
               |""".stripMargin
  override val from: ResultSet => BatchJobModel = { rs : ResultSet =>
    BatchJobModel(
    rs.getString(name),
      rs.getString(description),
      rs.getString(owner),
      rs.getBoolean(system),
      rs.getLong(creationTime),
      rs.getString(etl).parseJson.convertTo[BatchETL],
      rs.getString(exclusivityConfig).parseJson.convertTo[BatchJobExclusionConfig]
    )
  }

  override val to: BatchJobModel => Array[(String, Any)] = model=> Array((name,model.name), (description,model.description),
    (owner,model.owner),
    (system,model.system),
    (creationTime,model.creationTime),
    (etl,model.etl.toJson.toString()),
    (exclusivityConfig,model.exclusivityConfig.toJson.toString())
  )

}
