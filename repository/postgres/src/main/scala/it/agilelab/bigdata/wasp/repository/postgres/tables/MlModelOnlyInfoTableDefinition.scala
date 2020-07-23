package it.agilelab.bigdata.wasp.repository.postgres.tables

import java.sql.ResultSet

import it.agilelab.bigdata.wasp.models.MlModelOnlyInfo
import org.bson.BsonObjectId
import org.bson.types.ObjectId

object MlModelOnlyInfoTableDefinition extends TableDefinition[MlModelOnlyInfo,(String,String,Long)]{

  override def tableName: String = "ML_MODEL_ONY_INFO"

  val name = "name"
  val version = "version"
  val className = "class_name"
  val timestamp = "timestamp"
  val modelFileId = "model_file_id"
  val favorite = "favorite"
  val description = "description"

  override def columns: List[String] = List(name,version,className,timestamp,modelFileId,favorite,description)

  override def ddl: String = s"""CREATE TABLE IF NOT EXISTS $tableName (
                                |$name varchar NOT NULL,
                                |$version varchar,
                                |$className varchar,
                                |$timestamp bigint,
                                |$modelFileId varchar,
                                |$favorite boolean,
                                |$description varchar,
                                |PRIMARY KEY ($name,$version,$timestamp))
                                |""".stripMargin

  override def from: ResultSet => MlModelOnlyInfo = rs => MlModelOnlyInfo(
    rs.getString(name),
    rs.getString(version),
    rs.getOption[String](className),
    rs.getOption[Long](timestamp),
    rs.getOption[String](modelFileId).map(s=> new BsonObjectId(new ObjectId(s))),
    rs.getBoolean(favorite),
    rs.getString(description)
  )

  override def to: MlModelOnlyInfo => Array[(String, Any)] = model => Array(
    (name,model.name),
    (version,model.version),
    (className,model.className.orNull),
    (timestamp,model.timestamp.orNull),
    (modelFileId,model.modelFileId.map(_.getValue.toString).orNull),
    (favorite,model.favorite),
    (description,model.description)
  )

  override def conditionPrimaryKey: ((String, String,Long)) => Array[(String,Any)] = k=> Array((name,k._1),(version,k._2),(timestamp,k._3))

  override def primaryKeyFromObject: MlModelOnlyInfo => (String, String,Long) = m=>
    (m.name,m.version,m.timestamp.get)
}
