package it.agilelab.bigdata.wasp.core.models

import org.mongodb.scala.bson.{BsonDocument, BsonObjectId}

object TopicModel {
  val readerType = "topic"

  val metadata = """{"name": "metadata", "type": {
    "name": "metadata_fields",
    "type": "record",
      "fields": [
        {"name": "id", "type": "string"},
        {"name": "sourceId", "type": "string"},
        {"name": "arrivalTimestamp", "type": "long"},
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

  def generateField(namespace: String, name: String, ownSchema: Option[String]): String = {
    val schema = (Some(TopicModel.schema_base) :: ownSchema :: Nil).flatten.mkString(", ")
    generate(namespace, name, schema)
  }

  /**
    * Generate final schema for TopicModel. Use this method if you schema not have a field metadata.
    * @param ownSchema
    * @return
    */

  def generateMetadataAndField(namespace: String, name: String, ownSchema: Option[String]): String = {
    val schema = (Some(metadata) :: Some(TopicModel.schema_base) :: ownSchema :: Nil).flatten.mkString(", ")
    generate(namespace, name, schema)
  }

  private def generate(namespace: String, name: String, schema: String) = {
    s"""
    {"type":"record",
    "namespace":"${namespace}",
    "name":"${name}",
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
                      partitionKeyField: Option[String],
                      schema: Option[BsonDocument],
                      _id: Option[BsonObjectId] = None) extends Model {
  def getJsonSchema: String = schema.getOrElse(new BsonDocument).toJson
}