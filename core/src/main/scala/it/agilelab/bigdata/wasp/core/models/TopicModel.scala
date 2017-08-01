package it.agilelab.bigdata.wasp.core.models

import reactivemongo.bson.{BSONDocument, BSONObjectID}

object TopicModel {
  val readerType = "topic"

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
}

case class TopicModel(override val name: String,
                      creationTime: Long,
                      partitions: Int,
                      replicas: Int,
                      topicDataType: String, // avro, json, xml
                      schema: Option[BSONDocument],
                      _id: Option[BSONObjectID] = None) extends Model