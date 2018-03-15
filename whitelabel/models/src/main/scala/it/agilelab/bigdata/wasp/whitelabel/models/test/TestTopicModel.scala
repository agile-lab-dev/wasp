package it.agilelab.bigdata.wasp.whitelabel.models.test

import it.agilelab.bigdata.wasp.core.models._
import it.agilelab.bigdata.wasp.core.utils.JsonConverter

private[wasp] object TestTopicModel {

  private val topic_name = "test"
  private val topic2_name = "test2"

  lazy val json = TopicModel(
    name = TopicModel.name(topic_name + "_json"),
    creationTime = System.currentTimeMillis,
    partitions = 3,
    replicas = 1,
    topicDataType = "json",
    partitionKeyField = None,
    schema = JsonConverter.fromString(topicSchema).getOrElse(org.mongodb.scala.bson.BsonDocument())
  )

  lazy val json2 = TopicModel(
    name = TopicModel.name(topic2_name + "_json"),
    creationTime = System.currentTimeMillis,
    partitions = 3,
    replicas = 1,
    topicDataType = "json",
    partitionKeyField = None,
    schema = JsonConverter.fromString(topicSchema).getOrElse(org.mongodb.scala.bson.BsonDocument())
  )

  lazy val avro = TopicModel(
    name = TopicModel.name(topic_name + "_avro"),
    creationTime = System.currentTimeMillis,
    partitions = 3,
    replicas = 1,
    topicDataType = "avro",
    partitionKeyField = None,
    schema = JsonConverter.fromString(topicSchema).getOrElse(org.mongodb.scala.bson.BsonDocument())
  )

  lazy val avro2 = TopicModel(
    name = TopicModel.name(topic2_name + "_avro"),
    creationTime = System.currentTimeMillis,
    partitions = 3,
    replicas = 1,
    topicDataType = "avro",
    partitionKeyField = None,
    schema = JsonConverter.fromString(topicSchema).getOrElse(org.mongodb.scala.bson.BsonDocument())
  )

  private val topicSchema =
    TopicModel.generateField("test", "test", Some(
      """
        |        {
        |            "name": "id",
        |            "type": "string",
        |            "doc": ""
        |        },
        |        {
        |            "name": "number",
        |            "type": "int",
        |            "doc": ""
        |        },
        |        {
        |            "name": "nested",
        |            "type" : {
        |                         "type" : "record",
        |                         "name" : "nested_document",
        |                         "fields" : [
        |                            {"name" : "field1",
        |                             "type" : "string"},
        |
        |                            {"name" : "field2",
        |                             "type" : "long"},
        |
        |                            {"name" : "field3",
        |                             "type" : "string"}
        |                          ]
        |                      }
        |        }
      """.stripMargin))
}