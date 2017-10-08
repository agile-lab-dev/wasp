package it.agilelab.bigdata.wasp.core.models

import org.mongodb.scala.bson.{BsonDocument, BsonObjectId}

object TopicModel {
  val readerType = "topic"

  val metadata = """{"name": "metadata", "type": {
    "name": "metadata_fields",
    "type": "record",
      "fields": [
        {"name": "id", "type": "string"},
        {"name": "arrivalTimestamp", "type": "long"},
        {"name": "lat", "type": "double"},
        {"name": "lon", "type": "double"},
        {"name": "lastSeenTimestamp", "type": "long"},
        {"name": "path",
          "type": {
            "type": "array",
            "items": {
              "name": "Path",
              "type": "record",
              "fields": [
                {"name": "name", "type": "string"},
                {"name": "ts", "type": "long"}
              ]
            }
          }
        }
      ]
    }
  }"""

  val schema_base = """
      {"name":"id_event","type":"double"},
      {"name":"source_name","type":"string"},
      {"name":"topic_name","type":"string"},
      {"name":"metric_name","type":"string"},
      {"name":"timestamp","type":"string","java-class" : "java.util.Date"},
      {"name":"latitude","type":"double"},
      {"name":"longitude","type":"double"},
      {"name":"value","type":"double"},
      {"name":"payload","type":"string"}
  """

  def name(basename: String) = s"${basename.toLowerCase}.topic"

  /**
    * Generate final schema for TopicModel. Use this method if you schema have a field metadata.
    * @param ownSchema
    * @return
    */

  def generateField(ownSchema: Option[String]): String = {
    val schema = (Some(TopicModel.schema_base) :: ownSchema :: Nil).flatten.mkString(", ")
    s"""
    {"type":"record",
    "namespace":"Logging",
    "name":"Logging",
    "fields":[
      ${schema}
    ]}"""
  }

  /**
    * Generate final schema for TopicModel. Use this method if you schema not have a field metadata.
    * @param ownSchema
    * @return
    */

  def generateMetadataAndField(ownSchema: Option[String]): String = {
    val schema = (Some(metadata) :: Some(TopicModel.schema_base) :: ownSchema :: Nil).flatten.mkString(", ")
    s"""
    {"type":"record",
    "namespace":"Logging",
    "name":"Logging",
    "fields":[
      ${schema}
    ]}"""
  }
}

case class TopicModel(override val name: String,
                      creationTime: Long,
                      partitions: Int,
                      replicas: Int,
                      topicDataType: String, // avro, json, xml
                      schema: Option[BsonDocument],
                      _id: Option[BsonObjectId] = None) extends Model {
  def getJsonSchema: String = schema.getOrElse(new BsonDocument).toJson
}