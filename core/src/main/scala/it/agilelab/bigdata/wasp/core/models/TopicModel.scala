package it.agilelab.bigdata.wasp.core.models

import it.agilelab.bigdata.wasp.core.utils.SchemaConverters
import org.apache.avro.Schema
import org.apache.spark.sql.types.DataType
import org.mongodb.scala.bson.BsonDocument

object TopicModel {
  val readerType = "topic"

  def name(basename: String) = s"${basename.toLowerCase}.topic"

  /**
    * Generate final schema for TopicModel. Use this method if you schema have a field metadata.
    * @param ownSchema
    * @return
    */

  def generateField(namespace: String, name: String, ownSchema: Option[String]): String = {
    val schema = (ownSchema :: Nil).flatten.mkString(", ")
    generate(namespace, name, schema)
  }

  private def generate(namespace: String, name: String, schema: String) = {
    s"""
      {
        "type":"record",
        "namespace":"${namespace}",
        "name":"${name}",
        "fields":[
          ${schema}
        ]
      }
    """
  }
}

case class TopicModel(override val name: String,
                      creationTime: Long,
                      partitions: Int,
                      replicas: Int,
                      topicDataType: String, // avro, json, xml
                      partitionKeyField: Option[String],
                      schema: BsonDocument)
    extends Model {
  def getJsonSchema: String = schema.toJson
  def getDataType: DataType = {
    val schemaAvro = new Schema.Parser().parse(this.getJsonSchema)
    SchemaConverters.toSqlType(schemaAvro).dataType
  }
}