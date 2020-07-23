package it.agilelab.bigdata.wasp.repository.postgres.tables

import java.sql.ResultSet

import org.bson.types.ObjectId
import org.mongodb.scala.bson.{BsonDocument, BsonObjectId}
import spray.json.{JsValue, _}

object MlModelOnlyDataTableDefinition extends TableDefinition[(BsonObjectId,String,BsonDocument,Array[Byte]),BsonObjectId] {

  val id = "id"
  val fileName = "file_name"
  val data = "data"
  val metadata = "matadata"

  override def tableName: String = "ML_MODEL_ONY_DATA"

  override def columns: List[String] = List(id,fileName,metadata,data)

  override def ddl: String = s"""CREATE TABLE IF NOT EXISTS $tableName (
                                |$id serial primary key,
                                |$fileName varchar,
                                |$metadata json,
                                |$data BYTEA)
                                |""".stripMargin

  override def from: ResultSet => (BsonObjectId,String,BsonDocument,Array[Byte]) = rs => (
    new BsonObjectId(new ObjectId("%024d".format(rs.getLong(id)))),
    rs.getString(fileName),
    org.bson.BsonDocument.parse(rs.getString(metadata)),
    rs.getBytes(data)
  )


  override def to: ((BsonObjectId, String, BsonDocument, Array[Byte])) => Array[(String, Any)] =  elem => {
   val (_,_fileName,_metadata,_data) = elem
    Array(
      (fileName,_fileName),
      (metadata,_metadata.toJson.parseJson),
      (data,_data)
    )
  }


  override def conditionPrimaryKey: BsonObjectId => Array[(String, Any)] = l => Array((id,l.getValue.toString.toLong))

  override def primaryKeyFromObject: ((BsonObjectId, String, BsonDocument, Array[Byte])) => BsonObjectId = elem  => elem._1
}
